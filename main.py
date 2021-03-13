from azure.servicebus import ServiceBusClient, ServiceBusMessage
from datetime import datetime
from PIL import Image
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import requests, json, threading, time, os, uuid, base64, sys, queue, re, io

with open("credentials.json") as f:
    credentials = f.read()

RECIEVE_PLATES_CONNECTION_STR = credentials["RECIEVE_PLATES_CONNECTION_STR"]
RECIEVE_PLATES_TOPIC = "licenseplateread"
SUBSCRIPTION_KEY = credentials["SUBSCRIPTION_KEY"]

UPDATE_WANTED_CONNECTION_STR = credentials["UPDATE_WANTED_CONNECTION_STR"]
UPDATE_WANTED_TOPIC = "wantedplatelistupdate"
AUTH_HEADER = {'Authorization': 'Basic ' + credentials["AUTH_HEADER"]}

STORAGE_CONNECTION_STR = credentials["STORAGE_CONNECTION_STR"]


blob_service_client = BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STR)
container_name = "plateimages3b960b81-7433-4771-96a5-c3d9c57953e6"

gotUpdate = False
azureQueue = queue.Queue()
wantedPlates = ['003VLH', '006ZCC', '025WFD', '027SSD', '034XMW', '047PGM', '056TKC', '065TZN', '069VLW', '092PYB', '103EEN', '106WER', '106XBJ', '137XDD', '152WXQ', '160VGM', '170YYT', '172ZAZ', '186ZMX', '204PYE', '204TVD', '214SSX', '217TAP', '217VMT', '218ZLZ', '220WNZ', '241ZCK', '242JSC', '244WFB', '250TMW', '250ZHW', '251HLL', '251KLK', '252YRN', '259XYS', '260DJT', '262LTC', '276XYK', '279NHJ', '294GSE', '321WFD', '327WST', '330XNV', '333LBJ', '337PXD', '344XLY', '347WRK', '347WVL', '356WMW', '380RHW', '381TEX', '440JDR', '463WLP', '466SYK', '472VTN', '480CVR', '490RVQ', '498YRH', '501VYG', '509TLL', '518JLA', '523WMF', '529YHG', '533SLM', '543JBA', '552LFF', '557TRJ', '558GBN', '569SRT', '569XSC', '570JAS', '582MBF', '587SSM', '590XTC', '611VEZ', '623XXW', '638XBW', '643TEV', '660XPS', '664VJY', '673WAJ', '676ZAY', '678XXG', '690SRK', '694ZMY', '699WWZ', '701XFN', '726RVN', '729TEA', '737XPN', '740MZK', '744YHK', '755WXH', '762GGY', '766XEC', '768VJW', '770NEP', '798PPR', '799HLT', '810YRD', '817ZRH', '832LCT', '843NEE', '847JWG', '857LFT', '872ZMY', '874KBP', '879ZEQ', '884HBJ', '888ZSF', '898APD', '898JZZ', '898SGE', '909SDH', '922ZLX', '928RST', '929ZBW', '947ZZL', '964KBR', '966WLG', '967TKV', '972XVM', '998RXX', 'B36ABP', 'B51AGZ', 'B54BCR', 'B67AMR', 'D29AFH', 'D60BJQ', 'D69BEZ', 'D88ASJ', 'E23BBD', 'E78AVD', 'E97BAC', 'FAL5076', 'FDN3209', 'FEP7515', 'FEW4523', 'FEW9849', 'FEY8670', 'FEZ6158', 'FF17277', 'FFG3391', 'FFM9360', 'FFP8453', 'FGJ4436', 'FGJ6253', 'FGN6751', 'H04AGC', 'H04BRZ', 'H55AWD', 'H68ASR', 'H80BBD', 'H94BMJ', 'J55BGL', 'K19AFA', 'KD7ACT', 'L442237', 'L483573', 'M11ARC', 'M30BHN', 'M33BMX', 'M58BHP', 'N96AXX', 'P38AJR', 'P69AMW', 'P87AGM', 'P92BMW', 'P94AKV', 'RA0441P', 'RB3217Z', 'W79ANB', 'X73BBE', 'X91BDS', 'Y55ABN', 'Y57AGG', 'Y72BMW', 'Z43ASQ']


def resizeImg(imageBytes):
    basewidth = 50
    baseheight = 50
    img = Image.open(io.BytesIO(imageBytes))

    if img.size[0] < 50:
        wpercent = (basewidth / float(img.size[0]))
        hsize = int((float(img.size[1]) * float(wpercent)))
        img = img.resize((basewidth, hsize), Image.ANTIALIAS)
    elif img.size[1] < 50:
        hpercent = (baseheight / float(img.size[1]))
        wsize = int((float(img.size[0]) * float(hpercent)))
        img = img.resize((wsize, baseheight), Image.ANTIALIAS)
    else:
        return imageBytes

    img_byte_arr = io.BytesIO()
    img.save(img_byte_arr, format='JPEG')
    return img_byte_arr.getvalue()


class AzureOCR:
    OCR_ENDPOINT = "https://eastus.api.cognitive.microsoft.com/"
    OCR_HEADERS = {
        "Content-Type": "application/octet-stream",
        "Ocp-Apim-Subscription-Key": credentials["AZURE_SUBSCRIPTION_KEY"]
    }

    azureQueue = queue.Queue()

    def __init__(self, plate):
        self.waitingForResponse = False
        self.plate = plate
        self.image = resizeImg(base64.b64decode(plate["LicensePlateImageJpg"]))
        self.responseURL = ""
        self.licensePlate = []
        self.failed = False

    def startOCR(self):
        r = requests.post(AzureOCR.OCR_ENDPOINT + "vision/v2.0/recognizeText?mode=Printed", data=self.image, headers=AzureOCR.OCR_HEADERS)
        if r.status_code == 202:
            self.responseURL = r.headers["Operation-Location"]
            self.waitingForResponse = True
            #print("Started")
        else:
            print("(%i) Failed to start OCR processing for: %s" % (r.status_code, self.plate["LicensePlate"]))
            with open(self.plate["LicensePlate"] + ".jpg", "wb") as f:
                f.write(self.image)
            print(r.content)
            self.failed = True

    def completeOCR(self) -> bool:
        r = requests.get(self.responseURL, headers=AzureOCR.OCR_HEADERS)
        if r.status_code == 200:
            data = json.loads(r.content)
            if data["status"].lower() == "succeeded":
                #print(data)
                for line in data["recognitionResult"]["lines"]:
                    plate = re.sub(r'[^A-Za-z0-9 ]+', '', line["text"]).replace(" ", "")
                    if len(plate) == 6 or len(plate) == 7:
                        self.licensePlate.append(plate)
                return True
        else:
            print("(%i) Failed to fetch response OCR!" % (r.status_code))
            self.failed = True
        return False

def worker():
    global wantedPlates
    while True:
        item = AzureOCR.azureQueue.get()

        if not item.failed:
            if not item.waitingForResponse:
                item.startOCR()
                AzureOCR.azureQueue.put(item)
            else:
                if item.completeOCR():
                    for p in item.licensePlate:
                        if p in wantedPlates:
                            print("FoundA: " + p)
                            sendFoundWantedPlates(item.plate, p, True)
                else:
                    AzureOCR.azureQueue.put(item)

        AzureOCR.azureQueue.task_done()

def auth_shared_access_signature():
    from azure.storage.blob import BlobServiceClient
    # [START create_sas_token]
    # Create a SAS token to use to authenticate a new client
    from datetime import datetime, timedelta
    from azure.storage.blob import ResourceTypes, AccountSasPermissions, generate_account_sas

    return generate_account_sas(
        blob_service_client.account_name,
        account_key=blob_service_client.credential.account_key,
        resource_types=ResourceTypes(object=True),
        permission=AccountSasPermissions(read=True),
        expiry=datetime.utcnow() + timedelta(hours=1)
    )


def uploadImagePlate(plate) -> str:
    plate_file_name = plate["LicensePlate"] + str(uuid.uuid4()) + ".jpg"
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=plate_file_name)
    plate_data = base64.b64decode(plate["ContextImageJpg"])
    
    blob_client.upload_blob(plate_data)
    print("Uploaded plate image of: " + plate["LicensePlate"])
    source_blob = BlobClient(blob_service_client.url, container_name=container_name, blob_name=plate_file_name, credential=auth_shared_access_signature())
    return str(source_blob.url)

def getWantedPlates() -> list:
    api = "https://licenseplatevalidator.azurewebsites.net/api/lpr/wantedplates"
    r = requests.get(api, headers=AUTH_HEADER)
    data = []
    if r.status_code == 200:
        d = r.content.decode()
        l = d.split(",")
        for plate in l:
            p = str(plate).replace('"', "").replace("[", "").replace("]", "")
            data.append(p)
    else:
        print("(%i) Failed to retrieve list of wanted plates!" % r.status_code)
        sys.exit(0)

    print("Searching for: " + str(data))
    return data


def getUpdateNotification():
    global gotUpdate

    '''
    Technically this isnt really how you want to be doing multithreading.
    But since python doesn't truly have multithreading (due to GIL),
    it is possible to modify a variable without a mutex and not have race problems.
    '''

    while True:
        servicebus_client = ServiceBusClient.from_connection_string(conn_str=UPDATE_WANTED_CONNECTION_STR, logging_enable=True)
        with servicebus_client:
            receiver = servicebus_client.get_subscription_receiver(topic_name=UPDATE_WANTED_TOPIC, subscription_name=SUBSCRIPTION_KEY, max_wait_time=5)
            with receiver:
                for msg in receiver:
                    updateMsg = str(msg)
                    if "TotalWantedCount" in updateMsg:
                        print("\nUpdate found!")
                        gotUpdate = True
                    receiver.complete_message(msg)
                    if gotUpdate:
                        continue

fuzzyGroups = [
    ["B", "8"],
    ["C", "G"],
    ["E", "F"],
    ["K", "X", "Y"],
    ["I", "1", "T", "J"],
    ["S", "5"],
    ["O", "D", "Q", "0"],
    ["P", "R"],
    ["Z", "2"]
]

def isFuzzy(c1, c2) -> bool:
    for fuzzy in fuzzyGroups:
        if c1 in fuzzy and c2 in fuzzy:
            #print("Fuzzy match: %s %s" % (c1, c2))
            return True
    return False

def isMatchingPlate(plate, wanted) -> bool:
    isWanted = True
    for i in range(len(wanted)):
        if not (plate[i] == wanted[i] or isFuzzy(plate[i], wanted[i])):
            isWanted = False
            break
    return isWanted

def findPlates():
    global gotUpdate, wantedPlates
    print("Looking for plates...")

    servicebus_client = ServiceBusClient.from_connection_string(conn_str=RECIEVE_PLATES_CONNECTION_STR, logging_enable=True)
    with servicebus_client:
        receiver = servicebus_client.get_subscription_receiver(topic_name=RECIEVE_PLATES_TOPIC, subscription_name=SUBSCRIPTION_KEY, max_wait_time=5)
        with receiver:
            while not gotUpdate:
                for msg in receiver:
                    if gotUpdate:
                        return

                    plate = json.loads(str(msg))
                    found = False                        
                    for p in wantedPlates:
                        if isMatchingPlate(str(p), str(plate["LicensePlate"])):
                            print("Found: " + p)
                            sendFoundWantedPlates(plate, p)
                            found = True
                            break
                    if not found:
                        AzureOCR.azureQueue.put(AzureOCR(plate))
                    receiver.complete_message(msg)

def sendFoundWantedPlates(found, wantedNumber, isAzure=False):
    uploadedURI = uploadImagePlate(found)
    #print(str(uploadedURI))
    api = "https://licenseplatevalidator.azurewebsites.net/api/lpr/platelocation"
    payload = '''{
"LicensePlateCaptureTime": "%s",
"LicensePlate": "%s",
"Latitude": %s,
"Longitude": %s,
"ContextImageReference": "%s" }''' % (found["LicensePlateCaptureTime"], wantedNumber, str(found["Latitude"]), str(found["Longitude"]), str(uploadedURI))

    r = requests.post(api, data=payload, headers=AUTH_HEADER)
    timestamp = datetime.now().strftime("%Y-%m-%d, %H:%M:%S")
    if found["LicensePlate"] != wantedNumber:
        print("[%s] (%i) SentF: %s\n" % (timestamp, r.status_code, found["LicensePlate"]))
    elif isAzure:
        print("[%s] (%i) SentA: %s\n" % (timestamp, r.status_code, found["LicensePlate"]))
    else:
        print("[%s] (%i) Sent: %s\n" % (timestamp, r.status_code, found["LicensePlate"]))
    

def main():
    global gotUpdate, wantedPlates
    updateThread = threading.Thread(target=getUpdateNotification)
    updateThread.setDaemon(True)
    updateThread.start()

    azureOCRThread = threading.Thread(target=worker, daemon=True)
    azureOCRThread.start()

    #wantedPlates = getWantedPlates()
    #wantedPlates = ['003VLH', '025WFD', '027SSD', '047PGM', '065TZN', '172ZAZ', '217TAP', '241ZCK', '250TMW', '250ZHW', '259XYS', '262LTC', '279NHJ', '321WFD', '337PXD', '344XLY', '463WLP', '480CVR', '523WMF', '690SRK', '729TEA', '744YHK', '768VJW', '817ZRH', '898JZZ', '966WLG', '967TKV', 'B54BCR', 'D29AFH', 'D69BEZ', 'FEP7515', 'FEY8670', 'FF17277', 'H68ASR', 'L483573', 'M11ARC', 'M30BHN', 'M33BMX', 'M58BHP', 'P87AGM', 'P92BMW', 'X73BBE', 'Y72BMW']
    findPlates()
    while True:
        if gotUpdate:
            gotUpdate = False
            wantedPlates = getWantedPlates()
            findPlates()
        time.sleep(0.1)

    updateThread.join()

if __name__ == '__main__':
    main()

