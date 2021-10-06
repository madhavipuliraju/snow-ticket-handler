import logging
import requests
import time


logger = logging.getLogger()
logger.setLevel(logging.INFO)


def update_comments_and_resolve(message, sys_id, resolution, instance, snow_auth):
    """
    Posts a comment in the Servicenow ticket
    """
    create_incident_url = f"https://{instance}.service-now.com/api/now/table/incident/{sys_id}"
    data = {
        "comments": message
    }
    if resolution:
        data["state"] = "6"
    
    headers = {
        'Authorization': snow_auth,
        'Content-Type': 'application/json'
    }
    response = requests.request("PUT", create_incident_url,
                                json=data, headers=headers)
    if response.status_code == 200:
        logger.info(f"Updated snow ticket: {sys_id} with comments")
    else:
        logger.error(f"couldn't update snow ticket: {sys_id} API returned {response.status_code} and\n\n{response.text}")

def add_snow_attachment(instance, snow_auth, file_link, file_type, file_name, source_auth, user_mapping_table, key, pkey, from_haptik):
    """
    Adds Attachment to SNOW Ticket
    """
    for i in range(10):
        response = user_mapping_table.get_item(Key={key: pkey})
        if "Item" not in response:
            logger.error(f"Items not available for: {pkey}")
            return
        snow_system_id = response.get("Item", {}).get("snow_system_id")
        if snow_system_id:
            break
        logger.info(f"System id not found on trial {i}, waiting for a second before retrying")
        time.sleep(1)
    file_name = file_name.split(".")[0]

    add_attachment_url = f"https://{instance}.service-now.com/api/now/attachment/file?table_name=incident&table_sys_id={snow_system_id}&file_name={file_name}"

    file_headers = {
        "Authorization": source_auth
    }

    if file_type in ["png","jpg","image"]:
        if from_haptik:
            response = requests.get(file_link, stream=True)
        else:
            response = requests.get(file_link, headers = file_headers)
        data = response.content
        headers = {
            "Content-Type": "image/png",
            "Accept": "application/json",
            "Authorization": snow_auth
        }
        url = add_attachment_url + ".png"
        add_attachment_response = requests.post(url, headers=headers, data=data)
        if add_attachment_response.status_code == 201:
            logger.info(f"Successfully Added Image Attachement to SNOW sys_id: {snow_system_id}")
        else:
            logger.debug(f"Error occured while adding Image Attachement to SNOW sys_id: {snow_system_id}")
    elif file_type in ["pdf"]:
        response = requests.get(file_link, headers = file_headers)
        data = response.content
        headers = {
            "Content-Type": "application/pdf",
            "Accept": "application/json",
            "Authorization": snow_auth
        }
        url = add_attachment_url + ".pdf"
        add_attachment_response = requests.post(url, headers=headers, data=data)
        if add_attachment_response.status_code == 201:
            logger.info(f"Successfully Added PDF Attachement to SNOW sys_id: {snow_system_id}")
        else:
            logger.debug(f"Error occured while adding PDF Attachement to SNOW sys_id: {snow_system_id}")
    elif file_type in ["docx"]:
        response = requests.get(file_link, headers = file_headers)
        data = response.content
        headers = {
            "Content-Type": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            "Accept": "application/json",
            "Authorization": snow_auth
        }
        url = add_attachment_url + ".docx"
        add_attachment_response = requests.post(url, headers=headers, data=data)
        if add_attachment_response.status_code == 201:
            logger.info(f"Successfully Added Docx Attachement to SNOW sys_id: {snow_system_id}")
        else:
            logger.debug(f"Error occured while adding Docx Attachement to SNOW sys_id: {snow_system_id}")
    else:
        logger.error(f"Received other format of file, only accepts PNG, JPG, Image, PDF and Docx")