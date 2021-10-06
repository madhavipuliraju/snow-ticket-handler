import json
import boto3
import os
import logging
import requests
from ticketing_helper import update_comments_and_resolve, add_snow_attachment
from profiler import profile

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

db_service = boto3.resource('dynamodb')


lambda_client = boto3.client('lambda')


@profile
def lambda_handler(event, context):
    """
    # Data passed from Event Handlers via Invoke
    """
    logger.info(f"Incoming Event: {event}")
    email = event.get("email", "")
    message = event.get("message", "")
    client_id = event.get("client_id", "")
    source = event.get("source")
    incoming_event = event.get("event")
    is_automated = event.get("is_automated")
    chat_history = event.get("chat_history")
    from_haptik = event.get("from_haptik")
    file_link = event.get("file_link", "")
    file_type = event.get("file_type","")
    file_name = event.get("file_name", "attachment")

    client_mapping_table = db_service.Table(
        os.environ.get('client_mapping_table'))

    response = client_mapping_table.get_item(Key={"client_id": client_id})
    if "Item" not in response:
        logger.error(f"Client details not found for client: {client_id}")    
        return
    is_translation = response.get("Items", {}).get("is_translation", "")
    instance = response.get("Item", {}).get("snow_instance")
    snow_auth = response.get("Item", {}).get("snow_auth")

    if source == "slack":
        user_id = event.get("user")
        user_mapping_table = db_service.Table(os.environ.get('slack_mapping_table'))
        key = "user_id"
        pkey = user_id
        source_auth = response["Item"].get("slack_auth")
    elif source == "teams":
        conversation_id = event.get("conversation_id", "")
        user_mapping_table = db_service.Table(os.environ.get('teams_mapping_table'))
        key = "con_id"
        pkey = conversation_id
        teams_client_id = response["Item"].get("teams_client_id")
        teams_client_secret = response["Item"].get("teams_client_secret")
        teams_scope = response.get("Item").get("teams_scope")
        source_auth = get_teams_auth(teams_client_id, teams_client_secret, teams_scope)
    elif source == "zoom":
        user_id = event.get("user")
        user_mapping_table = db_service.Table(os.environ.get('zoom_mapping_table'))
        key = "user_id"
        pkey = user_id
        source_auth = response["Item"].get("zoom_auth")
    else:
        logger.error(f"Invalid source: {source}")
        return

    if incoming_event == "TICKET_CREATION":
        logger.info(f"Creating SNOW Ticket for user: {pkey}")
        # Creates a new ticket if not present for a user
        try:
            response = user_mapping_table.get_item(Key={key: pkey})
            if "Item" not in response:
                snow_ticket_id, snow_system_id = create_snow_ticket(message, email,
                                                                    instance, snow_auth)
            else:
                snow_ticket_id = response.get("Item", {}).get("snow_ticket_id")
                snow_system_id = response.get("Item", {}).get("snow_system_id")
                if not snow_system_id:
                    snow_ticket_id, snow_system_id = create_snow_ticket(message, email,
                                                                        instance, snow_auth)
                else:
                    return

                user_mapping_table.update_item(
                    Key={key: pkey},
                    UpdateExpression="set snow_system_id=:s, snow_ticket_id=:t",
                    ExpressionAttributeValues={
                        ":s": snow_system_id,
                        ":t": snow_ticket_id
                    }
                )
                logger.info("Updated Ticket Info in the database")
        except Exception as te:
            logger.error(f"[SNOW TICKET] Creation of ticket failed: {te}")
    elif incoming_event == "TICKET_RESOLUTION":
        logger.info(f"Resolving ticket for user: {pkey}")
        response = user_mapping_table.get_item(Key={key: pkey})
        if is_translation:
            chat_transcript = response.get("Item", {}).get("chat_transcript", "")
            chat_history = f"Enlish:\n{chat_history}\n\nUser Prefered Language:\n{chat_transcript}"
        handle_resolution_event(key, pkey, is_automated, user_mapping_table,
                                instance, chat_history, snow_auth)
        user_mapping_table.update_item(Key={key: pkey},
                                       UpdateExpression="remove chat_transcript")
    elif incoming_event == "TICKET_ATTACHMENT":
        logger.info(f"Ticket Attachment for user: {pkey}")  
        add_snow_attachment(instance, snow_auth, file_link, file_type, file_name, source_auth, user_mapping_table, key, pkey, from_haptik)
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }


def create_snow_ticket(message, email, instance, snow_auth):
    """
    # Creates a snow ticket
    """
    logger.info(f"Creating Ticket on SNOW")
    create_incident_url = f"https://{instance}.service-now.com/api/now/table/incident"
    data = {
        "short_description": message.title(),
        "caller_id": email
    }

    # TODO move auth to client mapping, #snow_auth
    headers = {
        'Authorization': snow_auth,
        'Content-Type': 'application/json'
    }
    logger.info(f"Ticket Payload: {data}")
    logger.info(f"Ticket Headers: {headers}")
    response = requests.request(
        "POST", create_incident_url, json=data, headers=headers)
    logger.info(f"Create Ticket Response: {response.json()}")
    if response.status_code == 201:
        return response.json().get("result", {}).get("number"), response.json().get("result", {}).get("sys_id")


def handle_resolution_event(key, pkey, is_automated, user_mapping_table, instance, chat_history, snow_auth):
    """
    Handles webhook_conversation_complete event
    """
    response = user_mapping_table.get_item(Key={key: pkey})
    if "Item" in response and response["Item"]:
        sys_id = response.get("Item", {}).get("snow_system_id")
        update_comments_and_resolve(chat_history, sys_id,
                                    True, instance, snow_auth)
        if not is_automated:
            logger.info(
                "Agent resolved the conversation. Commenting accordingly")
            agent_name = response.get("Item", {}).get("agent_name")
            update_comments_and_resolve(f"This conversation was resolved by {agent_name}",
                                        sys_id, False, instance, snow_auth)
        user_mapping_table.update_item(Key={key: pkey},
                                       UpdateExpression="remove snow_system_id, snow_ticket_id, agent_name")
    else:
        logger.error("Coudn't find the system_id, skipping comments updation")


def get_teams_auth(client_id, client_secret, scope):
    """
    Generates the auth token
    """
    url = os.environ.get('teams_auth_token_url')

    payload = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": scope
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    response = requests.request("POST", url, headers=headers, data=payload)
    if response.status_code == 200:
        return "Bearer " + response.json().get("access_token")
    else:
        logger.error(f"couldn't generate auth token:\n{response.text}")
