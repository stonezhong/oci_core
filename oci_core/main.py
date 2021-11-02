import oci
import tempfile
import json
import os
import time
import base64
import io
import logging
import fcntl
import errno
import contextlib
from cachetools import TTLCache

logger = logging.getLogger(__name__)

_REGION_ID_TO_NAME_DICT = {
    "IAD"           : "us-ashburn-1",         # US - IAD
    "PHX"           : "us-phoenix-1",         # US - PHX
}

def get_region_name(region):
    if region in _REGION_ID_TO_NAME_DICT:
        return _REGION_ID_TO_NAME_DICT[region]
    else:
        return region.replace("_", "-").lower()


def _shall_retry_signer(e):
    if isinstance(e, oci.exceptions.ServiceError):
        if e.status == 401:
            return True
        else:
            return False
    return False


def _get_region_endpoint(endpoint_dict, region):
    return None if region is None else endpoint_dict[region]

_signer_cache = TTLCache(1, 1800) # cache lives up to 30 minutes

def _get_signer(*args, **kwargs):
    with lock("/tmp/oci-signer-lock"):
        return _get_signer_unsafe(*args, **kwargs)

def _get_signer_unsafe(retry_count=5, sleep_interval=5):
    signer = _signer_cache.get("default")
    if signer is not None:
        return signer

    if retry_count < 1:
        raise ValueError(f"bad retry_count ({retry_count}), MUST >=1")
    for i in range(0, retry_count):
        try:
            signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
            _signer_cache['default'] = signer
            return signer

        except Exception as e:
            if i >= (retry_count - 1) or not _shall_retry_signer(e):
                logger.error("_get_signer failed for {} times, error is: {}, message is: {}, no more retrying...".format(
                    i+1, e,  str(e)
                ))
                raise

            logger.warning("_get_signer failed for {} times, error is: {}, message is: {}, retrying after {} seconds...".format(
                i+1, e,  str(e), sleep_interval
            ))
            time.sleep(sleep_interval)


# get oci client for a given service
def _get_oci_service_client(service_client_class, service_endpoint, config=None):
    # if config is None, then we are using instance principle
    if config is None:
        signer = _get_signer()
        client = service_client_class(
            {}, signer=signer, 
            service_endpoint=service_endpoint
        )
    else:
        client = service_client_class(config)
    return client


# Object Storage Service API
# get object storage client
def get_os_client(region, config=None):
    if region is None:
        service_endpoint = None
    else:
        service_endpoint = f"https://objectstorage.{get_region_name(region)}.oraclecloud.com"
    return _get_oci_service_client(
        oci.object_storage.ObjectStorageClient, 
        service_endpoint, 
        config=config
    )

# Data Flow API
# get dataflow client using instance principle
def get_df_client(region, config=None):
    if region is None:
        service_endpoint = None
    else:
        service_endpoint = f"https://dataflow.{get_region_name(region)}.oci.oraclecloud.com"
    return _get_oci_service_client(
        oci.data_flow.DataFlowClient, 
        service_endpoint, 
        config=config
    )

def _shall_retry(e):
    if isinstance(e, oci.exceptions.ServiceError):
        if e.status == 503:
            return True
        else:
            return False
    if isinstance(e, oci.exceptions.RequestException):
        return True
    if isinstance(e, oci._vendor.urllib3.exceptions.ProtocolError):
        return True
    return False

# upload file to object storage
def os_upload(os_client, local_filename, namespace, bucket, object_name, retry_count=5, sleep_interval=5):
    if retry_count < 1:
        raise ValueError(f"bad retry_count ({retry_count}), MUST >=1")
    for i in range(0, retry_count):
        try:
            _os_upload_no_retry(os_client, local_filename, namespace, bucket, object_name)
            return
        except Exception as e:
            if i >= (retry_count - 1) or not _shall_retry(e):
                logger.error("Upload object (namespace={}, bucket={}, object_name={}) failed for {} times, error is: {}, message is: {}, no more retrying...".format(
                    namespace, bucket, object_name, i+1, e,  str(e)
                ))
                raise
            logger.warning("Upload object (namespace={}, bucket={}, object_name={}) failed for {} times, error is: {}, message is: {}, retrying after {} seconds...".format(
                namespace, bucket, object_name, i+1, e,  str(e), sleep_interval
            ))
            time.sleep(sleep_interval)


def _os_upload_no_retry(os_client, local_filename, namespace, bucket, object_name):
    try:
        os_client.delete_object(namespace, bucket, object_name)
    except oci.exceptions.ServiceError as e:
        if e.status!=404:
            raise
    with open(local_filename, "rb") as f:
        os_client.put_object(namespace, bucket, object_name, f)

# upload dict to object storage
def os_upload_json(os_client, data, namespace, bucket, object_name, retry_count=5, sleep_interval=5):
    tmp_f = tempfile.NamedTemporaryFile(delete=False)
    tmp_f.write(json.dumps(data).encode('utf-8'))
    tmp_f.close()

    try:
        os_upload(os_client, tmp_f.name, namespace, bucket, object_name, retry_count=retry_count, sleep_interval=sleep_interval)
    finally:
        os.remove(tmp_f.name)

def os_download_to_memory(os_client, namespace, bucket, object_name, retry_count=5, sleep_interval=5, chunk_size=4194304):
    if retry_count < 1:
        raise ValueError(f"bad retry_count ({retry_count}), MUST >=1")
    for i in range(0, retry_count):
        try:
            return _os_download_to_memory_no_retry(os_client, namespace, bucket, object_name, chunk_size)
        except Exception as e:
            if i >= (retry_count - 1) or not _shall_retry(e):
                logger.error("Download object (namespace={}, bucket={}, object_name={}) failed for {} times, error is: {}, message is: {}, no more retrying...".format(
                    namespace, bucket, object_name, i+1, e,  str(e)
                ))
                raise
            logger.warning("Download object (namespace={}, bucket={}, object_name={}) failed for {} times, error is: {}, message is: {}, retrying after {} seconds...".format(
                namespace, bucket, object_name, i+1, e,  str(e), sleep_interval
            ))
            time.sleep(sleep_interval)


def _os_download_to_memory_no_retry(os_client, namespace, bucket, object_name, chunk_size):
    r = os_client.get_object(namespace, bucket, object_name)
    f = io.BytesIO()
    try:
        for chunk in r.data.raw.stream(chunk_size, decode_content=False):
            f.write(chunk)
        return f.getvalue()
    finally:
        f.close()

# download file from object storage
def os_download(os_client, local_filename, namespace, bucket, object_name, retry_count=5, sleep_interval=5, chunk_size=4194304):
    if retry_count < 1:
        raise ValueError(f"bad retry_count ({retry_count}), MUST >=1")
    for i in range(0, retry_count):
        try:
            _os_download_no_retry(os_client, local_filename, namespace, bucket, object_name, chunk_size)
            return
        except Exception as e:
            if i >= (retry_count - 1) or not _shall_retry(e):
                logger.error("Download object (namespace={}, bucket={}, object_name={}) failed for {} times, error is: {}, message is: {}, no more retrying...".format(
                    namespace, bucket, object_name, i+1, e,  str(e)
                ))
                raise
            logger.warning("Download object (namespace={}, bucket={}, object_name={}) failed for {} times, error is: {}, message is: {}, retrying after {} seconds...".format(
                namespace, bucket, object_name, i+1, e,  str(e), sleep_interval
            ))
            time.sleep(sleep_interval)


def _os_download_no_retry(os_client, local_filename, namespace, bucket, object_name, chunk_size):
    r = os_client.get_object(namespace, bucket, object_name)

    with open(local_filename, "wb") as f:
        for chunk in r.data.raw.stream(chunk_size, decode_content=False):
            f.write(chunk)


# read a json file from object storage, return the json object
def os_download_json(os_client, namespace, bucket, object_name, retry_count=5, sleep_interval=5):
    tmp_f = tempfile.NamedTemporaryFile(delete=False)
    tmp_f.close()

    try:
        os_download(os_client, tmp_f.name, namespace, bucket, object_name, retry_count=retry_count, sleep_interval=sleep_interval)
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
    if region is None:
        service_endpoint = None
    else:
        service_endpoint = f"https://objectstorage.{get_region_name(region)}.oraclecloud.com"
    signer = oci.auth.signers.InstancePrincipalsDelegationTokenSigner(delegation_token=delegation_token)
    client = oci.object_storage.ObjectStorageClient(
        {}, signer=signer, service_endpoint=service_endpoint
    )
    return client

def os_delete_object(os_client, namespace, bucket, object_name, retry_count=5, sleep_interval=5):
    if retry_count < 1:
        raise ValueError(f"bad retry_count ({retry_count}), MUST >=1")
    for i in range(0, retry_count):
        try:
            os_client.delete_object(namespace, bucket, object_name)
            return
        except Exception as e:
            if i >= (retry_count - 1) or not _shall_retry(e):
                logger.error("Delete object (namespace={}, bucket={}, object_name={}) failed for {} times, error is: {}, message is: {}, no more retrying...".format(
                    namespace, bucket, object_name,
                    i+1, e,  str(e)
                ))
                raise
            logger.warning("Delete object (namespace={}, bucket={}, object_name={}) failed for {} times, error is: {}, message is: {}, retrying after {} seconds...".format(
                namespace, bucket, object_name,
                i+1, e,  str(e), sleep_interval
            ))
            time.sleep(sleep_interval)


def os_has_object(os_client, namespace, bucket, object_name, retry_count=5, sleep_interval=5):
    if retry_count < 1:
        raise ValueError(f"bad retry_count ({retry_count}), MUST >=1")
    for i in range(0, retry_count):
        try:
            os_client.head_object(namespace, bucket, object_name)
            return True
        except Exception as e:
            if isinstance(e, oci.exceptions.ServiceError) and e.status == 404:
                return False
            if i >= (retry_count - 1) or not _shall_retry(e):
                logger.error("Head object (namespace={}, bucket={}, object_name={}) failed for {} times, error is: {}, message is: {}, no more retrying...".format(
                    namespace, bucket, object_name,
                    i+1, e,  str(e)
                ))
                raise
            logger.warning("Head object (namespace={}, bucket={}, object_name={}) failed for {} times, error is: {}, message is: {}, retrying after {} seconds...".format(
                namespace, bucket, object_name,
                i+1, e,  str(e), sleep_interval
            ))
            time.sleep(sleep_interval)


def list_objects_start_with(os_client, namespace, bucket, prefix, fields="name", retry_count=5, sleep_interval=5):
    if retry_count < 1:
        raise ValueError(f"bad retry_count ({retry_count}), MUST >=1")
    for i in range(0, retry_count):
        try:
            return oci.pagination.list_call_get_all_results_generator(
                os_client.list_objects, 
                'record', 
                namespace, bucket, prefix=prefix, 
                fields = fields,
            )
        except Exception as e:
            if i >= (retry_count - 1) or not _shall_retry(e):
                logger.error("List object (namespace={}, bucket={}, prefix={}) failed for {} times, error is: {}, message is: {}, no more retrying...".format(
                    namespace, bucket, prefix,
                    i+1, e,  str(e)
                ))
                raise
            logger.warning("List object (namespace={}, bucket={}, prefix={}) failed for {} times, error is: {}, message is: {}, retrying after {} seconds...".format(
                namespace, bucket, prefix,
                i+1, e,  str(e), sleep_interval
            ))
            time.sleep(sleep_interval)


# delete objects based on object name prefix
def os_delete_objects(os_client, namespace, bucket, prefix, retry_count=5, sleep_interval=5):
    for record in list_objects_start_with(
        os_client, namespace, bucket, prefix, fields="name", retry_count=retry_count, sleep_interval=sleep_interval
    ):
        os_delete_object(os_client, namespace, bucket, record.name, retry_count=retry_count, sleep_interval=sleep_interval)

def os_delete_object_if_exists(os_client, namespace, bucket, object_name, retry_count=5, sleep_interval=5):
    try:
        os_delete_object(os_client, namespace, bucket, record.name, retry_count=retry_count, sleep_interval=sleep_interval)
    except oci.exceptions.ServiceError as e:
        if e.status!=404:
            raise

def os_rename_object(os_client, namespace, bucket, source_name, new_name, retry_count=5, sleep_interval=5):
    if retry_count < 1:
        raise ValueError(f"bad retry_count ({retry_count}), MUST >=1")
    rod = oci.object_storage.models.RenameObjectDetails(
        source_name = source_name,
        new_name = new_name,
    )
    for i in range(0, retry_count):
        try:
            os_client.rename_object(namespace, bucket, rod)
            return
        except Exception as e:
            if i >= (retry_count - 1) or not _shall_retry(e):
                logger.error("Rename object (namespace={}, bucket={}, source_name={}, new_name={}) failed for {} times, error is: {}, message is: {}, no more retrying...".format(
                    namespace, bucket, source_name, new_name, i+1, e,  str(e)
                ))
                raise
            logger.warning("Rename object (namespace={}, bucket={}, source_name={}, new_name={}) failed for {} times, error is: {}, message is: {}, retrying after {} seconds...".format(
                namespace, bucket, source_name, new_name, i+1, e,  str(e), sleep_interval
            ))
            time.sleep(sleep_interval)

def os_copy_object(
    os_client, 
    src_namespace_name, src_bucket_name, src_object_name,
    dst_region, dst_namespace_name, dst_bucket_name, dst_object_name,
    retry_count=5, sleep_interval=5
):
    if retry_count < 1:
        raise ValueError(f"bad retry_count ({retry_count}), MUST >=1")
    cod = oci.object_storage.models.CopyObjectDetails(
        destination_region      = get_region_name(dst_region),
        destination_bucket      = dst_bucket_name,
        destination_namespace   = dst_namespace_name,
        destination_object_name = dst_object_name,
        source_object_name      = src_object_name
    )
    for i in range(0, retry_count):
        try:
            os_client.copy_object(
                src_namespace_name,
                src_bucket_name,
                cod
            )
            return
        except Exception as e:
            if i >= (retry_count - 1) or not _shall_retry(e):
                logger.error("Copy object (src_namespace_name={}, src_bucket_name={}, src_object_name={}, dst_region={}, dst_namespace_name={}, dst_bucket_name={}, dst_object_name={}) failed for {} times, error is: {}, message is: {}, no more retrying...".format(
                    src_namespace_name, src_bucket_name, src_object_name, dst_region, dst_namespace_name, dst_bucket_name, dst_object_name, i+1, e,  str(e)
                ))
                raise
            logger.warning("Copy object (src_namespace_name={}, src_bucket_name={}, src_object_name={}, dst_region={}, dst_namespace_name={}, dst_bucket_name={}, dst_object_name={}) failed for {} times, error is: {}, message is: {}, retrying after {} seconds...".format(
                src_namespace_name, src_bucket_name, src_object_name, dst_region, dst_namespace_name, dst_bucket_name, dst_object_name, i+1, e,  str(e), sleep_interval
            ))
            time.sleep(sleep_interval)

def os_rename_objects(os_client, namespace, bucket, prefix, new_name_cb, retry_count=5, sleep_interval=5):
    for record in list_objects_start_with(
        os_client, namespace, bucket, prefix, fields="name", retry_count=retry_count, sleep_interval=sleep_interval
    ):
        os_rename_object(os_client, namespace, bucket, source_name, new_name, retry_count=retry_count, sleep_interval=sleep_interval)

# For secret service

# Vault Service Secret Retrieval API
# get secret service client
def get_secrets_client(region=None, config=None):
    if region is None:
        service_endpoint = None
    else:
        service_endpoint = f"https://secrets.vaults.{get_region_name(region)}.oci.oraclecloud.com"
    return _get_oci_service_client(
        oci.secrets.SecretsClient, 
        service_endpoint,
        config=config
    )

# Vault Service Secret Management API
def get_vaults_client(region=None, config=None):
    if region is None:
        service_endpoint = None
    else:
        service_endpoint = f"https://vaults.{get_region_name(region)}.oci.oraclecloud.com"
    return _get_oci_service_client(
        oci.vault.VaultsClient, 
        service_endpoint,
        config=config
    )

def __encode_base64_content(content, encoding):
    content_bytes       = content.encode(encoding)          # '{"x": 1}'  --> b'{"x": 1}'
    message_b64_b       = base64.b64encode(content_bytes)   # b'{"x": 1}' --> b'eyJ4IjogMX0='
    content_s           = message_b64_b.decode("ascii")
    return content_s

def __decode_base64_content(content, encoding):
    content_bytes       = content.encode("ascii")
    message_b64_b       = base64.b64decode(content_bytes)
    content_s           = message_b64_b.decode(encoding)
    return content_s

def __encode_base64_bin_content(bin_content):
    # input: bytes, output: str
    b64_bytes           = base64.b64encode(bin_content)
    b64_s               = b64_bytes.decode("ascii")
    return b64_s

def __decode_base64_bin_content(content):
    # input: str, output: bytes
    b64_s               = content.encode("ascii")
    b64_bytes           = base64.b64decode(b64_s)
    return b64_bytes


def secrets_wait_for_secret_state(
    vaults_client, secret_id, target_state, 
    wait_interval=5, max_wait_count=100
):
    for i in range(0, max_wait_count):
        time.sleep(wait_interval)
        secret = vaults_client.get_secret(secret_id)
        if secret.data.lifecycle_state == target_state:
            return
    raise Exception(f"secret id={secret_id} not reached in state {target_state}, current state is: {secret.data.lifecycle_state}")

def secrets_read_value(secrets_client, secret_id, encoding="utf-8", is_binary=False):
    response = secrets_client.get_secret_bundle(secret_id)
    if is_binary:
        return __decode_base64_bin_content(response.data.secret_bundle_content.content)
    else:
        return __decode_base64_content(response.data.secret_bundle_content.content, encoding)

def secrets_read_value_by_name(secrets_client, vault_id, secret_name, encoding="utf-8", is_binary=False):
    response = secrets_client.get_secret_bundle_by_name(secret_name, vault_id)

    if is_binary:
        return __decode_base64_bin_content(response.data.secret_bundle_content.content)
    else:
        return __decode_base64_content(response.data.secret_bundle_content.content, encoding)

def secrets_list_secrets(vaults_client, compartment_id, vault_id, **kwargs):
    ret = {}
    for record in oci.pagination.list_call_get_all_results_generator(
        vaults_client.list_secrets, 'record', compartment_id, **kwargs
    ):
        if record.vault_id != vault_id:
            continue
        if record.lifecycle_state != "ACTIVE":
            continue
        secret_name = record.secret_name
        secret_id   = record.id
        if secret_name in ret:
            raise Exception(f"Duplicate secret name: {secret_name}")
        ret[secret_name] = secret_id
    return ret

def secrets_get_secret_info(vaults_client, compartment_id, vault_id, secret_name):
    for record in oci.pagination.list_call_get_all_results_generator(
        vaults_client.list_secrets, 'record', compartment_id
    ):
        if record.vault_id != vault_id:
            continue
        if record.secret_name != secret_name:
            continue
        return (record.id, record.lifecycle_state, )
    
    return (None, None, )

def secrets_create_value(vaults_client, compartment_id, vault_id, 
    secret_name, secret_content, key_id, secret_description=None, encoding="utf-8",
    is_binary=False
):
    composite = oci.vault.VaultsClientCompositeOperations(vaults_client)
    if is_binary:
        content = __encode_base64_bin_content(secret_content)
    else:
        content = __encode_base64_content(secret_content, encoding)
    secret_content_details = oci.vault.models.Base64SecretContentDetails(
        content_type=oci.vault.models.SecretContentDetails.CONTENT_TYPE_BASE64,
        name=secret_name,
        stage="CURRENT",
        content=content
    )
    secrets_details = oci.vault.models.CreateSecretDetails(
        compartment_id=compartment_id,
        description = secret_description, 
        secret_content=secret_content_details,
        secret_name=secret_name,
        vault_id=vault_id,
        key_id=key_id)
    composite.create_secret_and_wait_for_state(
        create_secret_details=secrets_details,
        wait_for_states=[oci.vault.models.Secret.LIFECYCLE_STATE_ACTIVE]
    )

def secrets_update_value(vaults_client, secret_id, secret_content, encoding="utf-8", is_binary=False):
    composite = oci.vault.VaultsClientCompositeOperations(vaults_client)
    if is_binary:
        content = __encode_base64_bin_content(secret_content)
    else:
        content = __encode_base64_content(secret_content, encoding)
    secret_content_details = oci.vault.models.Base64SecretContentDetails(
        content_type=oci.vault.models.SecretContentDetails.CONTENT_TYPE_BASE64,
        stage="CURRENT",
        content=content
    )
    secrets_details = oci.vault.models.UpdateSecretDetails(secret_content=secret_content_details)
    composite.update_secret_and_wait_for_state(
        secret_id, 
        secrets_details,
        wait_for_states=[oci.vault.models.Secret.LIFECYCLE_STATE_ACTIVE]
    )

def secrets_delete_value(vaults_client, secret_id):
    secret_deletion_details = oci.vault.models.ScheduleSecretDeletionDetails(
        time_of_deletion=None
    )
    vaults_client.schedule_secret_deletion(secret_id, secret_deletion_details)
    secrets_wait_for_secret_state(vaults_client, secret_id, "PENDING_DELETION")


def secrets_undelete_value(vaults_client, secret_id):
    vaults_client.cancel_secret_deletion(secret_id)
    secrets_wait_for_secret_state(vaults_client, secret_id, "ACTIVE")
    return "ACTIVE"

def secrets_create_or_update_value(vaults_client, compartment_id, vault_id, 
    secret_name, secret_content, key_id, encoding="utf-8", is_binary=False
):
    secret_id, secret_state = secrets_get_secret_info(
        vaults_client, compartment_id, vault_id, secret_name
    )
    if secret_id is None:
        secrets_create_value(
            vaults_client, compartment_id, vault_id, 
            secret_name, secret_content, key_id, 
            encoding=encoding, is_binary=is_binary
        )
        return

    if secret_state == "PENDING_DELETION":
        secret_state = secrets_undelete_value(vaults_client, secret_id)

    if secret_state == "ACTIVE":
        secrets_update_value(
            vaults_client, secret_id, secret_content, encoding=encoding, is_binary=is_binary
        )
        return
    
    raise Exception(f"Unable to set secret value: name = {secret_name}, state={secret_state}")


@contextlib.contextmanager
def lock(lock_filename, wait_duration=600, sleep_duration=1):
    f = open(lock_filename, 'a')
    lock_acquired = False
    start = time.time()
    try:
        while True:
            try:
                fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
                lock_acquired = True
            except IOError as e:
                if e.errno != errno.EAGAIN:
                    raise
                if time.time() - start > wait_duration:
                    raise
                time.sleep(sleep_duration)
                continue
            yield
            break
    finally:
        if lock_acquired:
            fcntl.flock(f, fcntl.LOCK_UN)
        f.close()
