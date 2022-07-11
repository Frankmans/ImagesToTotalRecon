from PIL import Image
from pillow_heif import register_heif_opener
import os.path
import pandas as pd
import pickle
import os
import io
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.http import MediaIoBaseDownload
import pygsheets
import re
import shutil


# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/drive.metadata',
          'https://www.googleapis.com/auth/drive',
          'https://www.googleapis.com/auth/drive.file'
          ]


def write_gsheets(df, service_file, recon_sheet, total_recon_new_line=2):
    gc = pygsheets.authorize(service_file=service_file)

    # open the google spreadsheet (where 'PY to Gsheet Test' is the name of my sheet)
    sh = gc.open(recon_sheet)

    # select the first sheet
    wks = sh[0]

    df1 = pd.DataFrame()
    df2 = pd.DataFrame()
    df3 = pd.DataFrame()
    df4 = pd.DataFrame()
    df5 = pd.DataFrame()
    df1['id'] = df['id']
    df2['title'] = df['title']
    df3['lat'] = df['lat']
    df3['lng'] = df['lng']
    df3['status'] = df['status']
    df4['candidateimageurl'] = df['candidateimageurl']
    df5['hyperlink'] = df['hyperlink']

    # update the first sheet with df, starting at cell B2.
    wks.set_dataframe(df1, (total_recon_new_line, 1), copy_head=False)
    wks.set_dataframe(df2, (total_recon_new_line, 3), copy_head=False)
    wks.set_dataframe(df3, (total_recon_new_line, 5), copy_head=False)
    wks.set_dataframe(df4, (total_recon_new_line, 11), copy_head=False)
    wks.set_dataframe(df5, (total_recon_new_line, 13), copy_head=False)


def get_gdrive_service():
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
    # return Google Drive API service
    return build('drive', 'v3', credentials=creds)


def get_exif(filename):
    image = Image.open(filename)
    image.verify()
    return image.getexif().get_ifd(0x8825)


def get_geotagging(exif):
    geo_tagging_info = {}
    if not exif:
        raise ValueError("No EXIF metadata found")
    else:
        gps_keys = ['GPSVersionID', 'GPSLatitudeRef', 'GPSLatitude', 'GPSLongitudeRef', 'GPSLongitude',
                    'GPSAltitudeRef', 'GPSAltitude', 'GPSTimeStamp', 'GPSSatellites', 'GPSStatus', 'GPSMeasureMode',
                    'GPSDOP', 'GPSSpeedRef', 'GPSSpeed', 'GPSTrackRef', 'GPSTrack', 'GPSImgDirectionRef',
                    'GPSImgDirection', 'GPSMapDatum', 'GPSDestLatitudeRef', 'GPSDestLatitude', 'GPSDestLongitudeRef',
                    'GPSDestLongitude', 'GPSDestBearingRef', 'GPSDestBearing', 'GPSDestDistanceRef', 'GPSDestDistance',
                    'GPSProcessingMethod', 'GPSAreaInformation', 'GPSDateStamp', 'GPSDifferential']

        for k, v in exif.items():
            try:
                geo_tagging_info[gps_keys[k]] = str(v)
            except IndexError:
                pass
        return geo_tagging_info


def get_image_id(gdrive_list, filename):
    gdrive_link = ''

    for gdrive_file in gdrive_list:
        if filename == gdrive_file['name']:
            gdrive_link = "https://drive.google.com/file/d/" + gdrive_file['id'] + '/view'

    return gdrive_link


def get_all_images(path, valid_images, gdrive_list, total_recon_new_line=1):
    imgs = pd.DataFrame(columns=['id','title', 'lat', 'lng', 'status', 'candidateimageurl', 'hyperlink'])
    id = total_recon_new_line - 1
    for f in os.listdir(path):
        ext = os.path.splitext(f)[1]
        if ext.lower() not in valid_images:
            continue
        else:
            image_info = get_exif(os.path.join(path, f))
            results = get_geotagging(image_info)
            long, lat = convert_long_lat_decimal(results['GPSLongitude'], results['GPSLatitude'])
            candidateimageurl = get_image_id(gdrive_list, f)
            hyperlink = "https://intel.ingress.com/intel/?z=19&ll=" + str(lat) + "," + str(long)
            imgs.loc[imgs.shape[0]] = [id, f, lat, long, 'potential', candidateimageurl, hyperlink]
            id +=1

    return imgs


def convert_long_lat_decimal(long, lat):
    long_decimal = convert_long_lat_string(long)
    lat_decimal = convert_long_lat_string(lat)
    return long_decimal, lat_decimal


def convert_long_lat_string(string):
    splitted = string.split(',')
    splitted_float = [float(re.sub("[^\d\.]", "", i)) for i in splitted]
    return splitted_float[0] + splitted_float[1]/60 + splitted_float[2]/3600


def get_file_list(service, folder_id, images_list):
    items = []
    pageToken = ""
    while pageToken is not None:

        for i in images_list:
            response = service.files().list(q="'" + folder_id + "' in parents and mimeType='image/" + i.split('.')[1] + "'", pageSize=1000, pageToken=pageToken,
                                            fields="nextPageToken, files(id, name, mimeType)").execute()
            items.extend(response.get('files', []))
            pageToken = response.get('nextPageToken')

    return items


def file_downloader(service, file_list, temp_save_path):
    for i in file_list:
        file_download(service, i, temp_save_path)


def file_download(service, file_id, temp_save_path):
    request = service.files().get_media(fileId=file_id['id'])
    fh = io.BytesIO()

    # Initialise a downloader object to download the file
    downloader = MediaIoBaseDownload(fh, request, chunksize=4194304)
    done = False

    try:
        # Download the data in chunks
        while not done:
            status, done = downloader.next_chunk()

        fh.seek(0)

        # Write the received data to the file
        with open(os.path.join(temp_save_path, file_id['name']), 'wb') as f:
            shutil.copyfileobj(fh, f)

        print("File " + file_id['name'] + " downloaded")
        # Return True if file Downloaded successfully
        return True

    except:
        # Return False if something went wrong
        print("Something went wrong with: " + file_id['name'])
        return False


def remove_temp_files(path, file_list):
    for file in file_list:
        os.remove(os.path.join(path, file['name']))
        print("Deleted " + file['name'])


if __name__ == '__main__':
    register_heif_opener()
    temp_save_path = r'C:\ImagesTemp'
    topFolderId = 'blabla' # Please set the folder of the top folder ID, in gdrive copy the part from URL after folders/.
    service_file = 'blabla.json' # Create a developer project and provide access to a service account within that project to the gdrive you need it to use, follow this guide: https://developers.google.com/drive/api/quickstart/python
    total_recon_sheet = 'Total Recon' #Name of the google sheet you want to use with total recon.
    total_recon_new_line = 2 # Line which you want to start adding pictures to.
    images_list = [".jpeg", ".jpg", ".gif", ".png", ".tga", ".heif", ".heic"]

    service = get_gdrive_service()
    file_list = get_file_list(service, topFolderId, images_list)
    file_downloader(service, file_list, temp_save_path)
    df = get_all_images(temp_save_path, images_list, file_list, total_recon_new_line)
    if not df.empty:
        write_gsheets(df, service_file, total_recon_sheet, total_recon_new_line)

    remove_temp_files(temp_save_path, file_list)