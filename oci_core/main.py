import oci
import tempfile
import json
import os
import time

_OS_ENDPOINTS = {
    "PHX"           : "https://objectstorage.us-phoenix-1.oraclecloud.com",
    "IAD"           : "https://objectstorage.us-ashburn-1.oraclecloud.com",
    "UK_LONDON_1"   : "https://objectstorage.uk-london-1.oraclecloud.com",
    "EU_FRANKFURT_1": "https://objectstorage.eu-frankfurt-1.oraclecloud.com",
    "AP_SEOUL_1"    : "https://objectstorage.ap-seoul-1.oraclecloud.com",
    "CA_TORONTO_1"  : "https://objectstorage.ca-toronto-1.oraclecloud.com",
    "AP_TOKYO_1"    : "https://objectstorage.ap-tokyo-1.oraclecloud.com",
    "AP_MUMBAI_1"   : "https://objectstorage.ap-mumbai-1.oraclecloud.com",
    "SA_SAOPAULO_1" : "https://objectstorage.sa-saopaulo-1.oraclecloud.com",
    "AP_SYDNEY_1"   : "https://objectstorage.ap-sydney-1.oraclecloud.com",
    "EU_ZURICH_1"   : "https://objectstorage.eu-zurich-1.oraclecloud.com",
    "AP_MELBOURNE_1": "https://objectstorage.ap-melbourne-1.oraclecloud.com", 
    "AP_OSAKA_1"    : "https://objectstorage.ap-osaka-1.oraclecloud.com", 
    "EU_AMSTERDAM_1": "https://objectstorage.eu-amsterdam-1.oraclecloud.com", 
    "ME_JEDDAH_1"   : "https://objectstorage.me-jeddah-1.oraclecloud.com", 
    "AP_HYDERABAD_1": "https://objectstorage.ap-hyderabad-1.oraclecloud.com", 
    "CA_MONTREAL_1" : "https://objectstorage.ca-montreal-1.oraclecloud.com",
    "AP_CHUNCHEON_1": "https://objectstorage.ap-chuncheon-1.oraclecloud.com",
    "US_SANJOSE_1"  : "https://objectstorage.us-sanjose-1.oraclecloud.com",
    "ME_DUBAI_1"    : "https://objectstorage.me-dubai-1.oraclecloud.com",
    "UK_CARDIFF_1"  : "https://objectstorage.uk-cardiff-1.oraclecloud.com",
}

_DF_ENDPOINTS = {
    "PHX"           : "https://dataflow.us-phoenix-1.oci.oraclecloud.com",
    "IAD"           : "https://dataflow.us-ashburn-1.oci.oraclecloud.com",
    "UK_LONDON_1"   : "https://dataflow.uk-london-1.oci.oraclecloud.com",
    "EU_FRANKFURT_1": "https://dataflow.eu-frankfurt-1.oci.oraclecloud.com",
    "AP_SEOUL_1"    : "https://dataflow.ap-seoul-1.oci.oraclecloud.com",
    "CA_TORONTO_1"  : "https://dataflow.ca-toronto-1.oci.oraclecloud.com",
    "AP_TOKYO_1"    : "https://dataflow.ap-tokyo-1.oci.oraclecloud.com",
    "AP_MUMBAI_1"   : "https://dataflow.ap-mumbai-1.oci.oraclecloud.com",
    "SA_SAOPAULO_1" : "https://dataflow.sa-saopaulo-1.oci.oraclecloud.com",
    "AP_SYDNEY_1"   : "https://dataflow.ap-sydney-1.oci.oraclecloud.com",
    "EU_ZURICH_1"   : "https://dataflow.eu-zurich-1.oci.oraclecloud.com",
    "AP_MELBOURNE_1": "https://dataflow.ap-melbourne-1.oci.oraclecloud.com",
    "AP_OSAKA_1"    : "https://dataflow.ap-osaka-1.oci.oraclecloud.com",
    "EU_AMSTERDAM_1": "https://dataflow.eu-amsterdam-1.oci.oraclecloud.com",
    "ME_JEDDAH_1"   : "https://dataflow.me-jeddah-1.oci.oraclecloud.com",
    "AP_HYDERABAD_1": "https://dataflow.ap-hyderabad-1.oci.oraclecloud.com",
    "CA_MONTREAL_1" : "https://dataflow.ca-montreal-1.oci.oraclecloud.com",
    "AP_CHUNCHEON_1": "https://dataflow.ap-chuncheon-1.oci.oraclecloud.com",
    "US_SANJOSE_1"  : "https://dataflow.us-sanjose-1.oci.oraclecloud.com",
    # "ME_DUBAI_1"    : None,
    # "UK_CARDIFF_1"  : None,
}

_READ_CHUNK_SIZE = 4*1024*1024


# get object storage client using instance principle
def get_os_client(region, config=None):
    # if config is None, then we are using instance principle
    if config is None:
        signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
        client = oci.object_storage.ObjectStorageClient(
            {}, signer=signer, service_endpoint=_OS_ENDPOINTS[region]
        )
    else:
        client = oci.object_storage.ObjectStorageClient(
            config
        )
    return client

# get dataflow client using instance principle
def get_df_client(region, config=None):
    if config is None:
        signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
        client = oci.data_flow.DataFlowClient(
            {}, signer=signer, service_endpoint=_DF_ENDPOINTS[region]
        )
    else:
        client = oci.data_flow.DataFlowClient(config)
    return client

# upload file to object storage
def os_upload(os_client, local_filename, namespace, bucket, object_name, retry_count=5, sleep_interval=5):
    if retry_count < 1:
        raise ValueError(f"bad retry_count ({retry_count}), MUST >=1")
    for i in range(0, retry_count):
        try:
            _os_upload_no_retry(os_client, local_filename, namespace, bucket, object_name)
            return
        except (oci.exceptions.ServiceError, oci.exceptions.RequestException, oci._vendor.urllib3.exceptions.ProtocolError,) as e:
            if i >= (retry_count - 1):
                print("Upload object {} failed for {} times, error is: {}, message is: {}, no more retrying...".format(
                    object_name, i+1, e,  str(e)
                ))
                raise
            print("Upload object {} failed for {} times, error is: {}, message is: {}, retrying after {} seconds...".format(
                object_name, i+1, e,  str(e), sleep_interval
            ))
            time.sleep(sleep_interval)


def _os_upload_no_retry(os_client, local_filename, namespace, bucket, object_name):
    os_delete_object_if_exists(os_client, namespace, bucket, object_name)
    with open(local_filename, "rb") as f:
        os_client.put_object(namespace, bucket, object_name, f)

# upload dict to object storage
def os_upload_json(os_client, data, namespace, bucket, object_name):
    tmp_f = tempfile.NamedTemporaryFile(delete=False)
    tmp_f.write(json.dumps(data).encode('utf-8'))
    tmp_f.close()

    try:
        os_upload(os_client, tmp_f.name, namespace, bucket, object_name)
    finally:
        os.remove(tmp_f.name)

# download file from object storage
def os_download(os_client, local_filename, namespace, bucket, object_name, retry_count=5, sleep_interval=5):
    if retry_count < 1:
        raise ValueError(f"bad retry_count ({retry_count}), MUST >=1")
    for i in range(0, retry_count):
        try:
            _os_download_no_retry(os_client, local_filename, namespace, bucket, object_name)
            return
        except (oci.exceptions.ServiceError, oci.exceptions.RequestException, oci._vendor.urllib3.exceptions.ProtocolError,) as e:
            if i >= (retry_count - 1):
                print("Download object {} failed for {} times, error is: {}, message is: {}, no more retrying...".format(
                    object_name, i+1, e,  str(e)
                ))
                raise
            print("Download object {} failed for {} times, error is: {}, message is: {}, retrying after {} seconds...".format(
                object_name, i+1, e,  str(e), sleep_interval
            ))
            time.sleep(sleep_interval)


def _os_download_no_retry(os_client, local_filename, namespace, bucket, object_name):
    r = os_client.get_object(namespace, bucket, object_name)

    with open(local_filename, "wb") as f:
        for chunk in r.data.raw.stream(_READ_CHUNK_SIZE, decode_content=False):
            f.write(chunk)


# read a json file from object storage, return the json object
def os_download_json(os_client, namespace, bucket, object_name):
    tmp_f = tempfile.NamedTemporaryFile(delete=False)
    tmp_f.close()

    try:
        os_download(os_client, tmp_f.name, namespace, bucket, object_name)
        with open(tmp_f.name) as f:
            return json.load(f)
    finally:
        os.remove(tmp_f.name)

def get_delegation_token(spark):
    conf = spark.sparkContext.getConf()
    token_path = conf.get("spark.hadoop.fs.oci.client.auth.delegationTokenPath")

    # read in token
    with open(token_path) as fd:
        delegation_token = fd.read()
    return delegation_token


# get object storage client from dataflow app, using delegation token
def dfapp_get_os_client(region, delegation_token):
    signer = oci.auth.signers.InstancePrincipalsDelegationTokenSigner(delegation_token=delegation_token)
    client = oci.object_storage.ObjectStorageClient(
        {}, signer=signer, service_endpoint=_OS_ENDPOINTS[region]
    )
    return client

# delete objects based on object name prefix
def os_delete_objects(os_client, namespace, bucket, prefix):
    for record in oci.pagination.list_call_get_all_results_generator(
        os_client.list_objects, 'record', namespace, bucket, prefix=prefix, fields = "name",
    ):
        os_client.delete_object(namespace, bucket, record.name)

def os_delete_object_if_exists(os_client, namespace, bucket, object_name):
    try:
        os_client.delete_object(namespace, bucket, object_name)
    except oci.exceptions.ServiceError as e:
        if e.status!=404:
            raise

def os_rename_objects(os_client, namespace, bucket, prefix, new_name_cb):
    for record in oci.pagination.list_call_get_all_results_generator(
        os_client.list_objects, 'record', namespace, bucket, prefix=prefix, fields = "name",
    ):
        rod = oci.object_storage.models.RenameObjectDetails(
            new_name = new_name_cb(record.name),
            source_name = record.name,
        )
        os_client.rename_object(namespace, bucket, rod)

def os_get_endpoint(region):
    return _OS_ENDPOINTS[region]
