import requests
import asyncio
import logging
import config_urls as URL
from xml.etree import ElementTree

async def fetch_project_resource(sessionID, user, project, BlueAntheaders, ns):
    try:
        BlueAntSoapPayload = f"""<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:project="http://project.blueant.axis.proventis.net/" xmlns:base="http://base.blueant.axis.proventis.net/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
            <soapenv:Header/>
            <soapenv:Body>
                <project:getProjectResourcesRequestParameter>
                    <project:sessionID>{sessionID}</project:sessionID>
                    <project:projectID>{project}</project:projectID>
                    <project:releaseType>all</project:releaseType>
                    <project:personID>{user}</project:personID>
                </project:getProjectResourcesRequestParameter>
            </soapenv:Body>
        </soapenv:Envelope>"""

        BlueAntProjectResourceResponse = await asyncio.to_thread(
            lambda: requests.post(URL.BLUEANT_PROJECTSERVICE, headers=BlueAntheaders, data=BlueAntSoapPayload)
        )
        
        BlueAntProjectRessourceTextXML = BlueAntProjectResourceResponse.text
        
        tree = ElementTree.fromstring(BlueAntProjectRessourceTextXML)
        project_id_element = ElementTree.Element('ns1:projectid')
        project_id_element.text = str(project)

        if tree.find('.//{http://project.blueant.axis.proventis.net/}resourceID') is not None:
            project_resource_element = tree.find('.//ns9:ProjectResource', namespaces=ns)
            project_resource_element.append(project_id_element)
            logging.info(f"The response from the Blue Ant API {URL.BLUEANT_PROJECTSERVICE} for the user {user} and the project {project} was successfully stored in the XML.")
        else: 
            logging.info(f"The response from the Blue Ant API {URL.BLUEANT_PROJECTSERVICE} for the user {user} and the project {project} is empty.")
        
        return tree

    except Exception as e:
        logging.error(str(e))
        return None