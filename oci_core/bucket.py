import logging
logger = logging.getLogger(__name__)

from typing import Optional, List, Generator, Callable, Tuple
import time
import os

from oci_core import list_objects_start_with, os_upload_json, os_download_json_with_etag, os_download, os_upload, os_copy_object
from oci.object_storage.object_storage_client import ObjectStorageClient
from oci.object_storage.models import ObjectSummary
import oci

class Bucket:
    os_client: Optional[ObjectStorageClient]
    region: str
    namespace_name: str
    bucket_name: str

    ##############################################################
    # Deal with JSON object
    #     get_json
    #     create_or_update_json
    #     update_json
    #     create_json
    #     transform_json
    #
    # Deal with binary object
    #     download_object
    #     create_or_update_object
    #     upload_dir
    #     copy_object
    ##############################################################

    def __init__(self, *, 
                 os_client:Optional[ObjectStorageClient], 
                 region: str,
                 namespace_name:str, 
                 bucket_name:str) -> None:
        self.os_client = os_client
        self.region = region
        self.namespace_name = namespace_name
        self.bucket_name = bucket_name
    
    def list(self, *, 
             prefix:Optional[str]=None, 
             fields:List[str]=["name"],
             retry_count:float=5, 
             sleep_interval:float=5
            ) -> Generator[ObjectSummary, None, None]:
        """List all objects.

        Parameters:
            prefix: if present, only object whose name match the prefix will be returned.
            fields: specify list of fields to return, for example: ['name','size','timeCreated','etag']
            retry_count: number of time to retry for recoverable error
            sleep_interval: gap between retries
        """
        return list_objects_start_with(
            self.os_client, 
            self.namespace_name, 
            self.bucket_name, 
            prefix,
            fields=','.join(fields),
            retry_count=retry_count,
            sleep_interval=sleep_interval
        )
    
    def get_json(self, *, object_name:str, etag:Optional[str]=None, retry_count:float=5, sleep_interval:float=5) -> Tuple[dict, str]:
        """Get a JSON object.

        Parameters:
            object_name: name of the object
            etag: if present, the API will fail if etag does not match. You can also specify "*" to match any etag.
            retry_count: number of time to retry for recoverable error
            sleep_interval: gap between retries
        
        Returns:
            A tuple, first element is data for the JSON, 2nd element is the etag. 
            If object does not exist, both data and etag are None
        """
        try:
            return  os_download_json_with_etag(
                self.os_client, 
                self.namespace_name,
                self.bucket_name,
                object_name,
                retry_count=retry_count,
                sleep_interval=sleep_interval,
                opts={"if_match":etag}
            )
        except oci.exceptions.ServiceError as e:
            if e.status==404:
                return None, None
            else:
                raise

    def create_or_update_json(self, *, object_name:str, data:dict, retry_count:float=5, sleep_interval:float=5) -> str:
        """Create a new JSON object or update an existing JSON object.

        Parameters:
            object_name: name of the object
            data: the json data
            retry_count: number of time to retry for recoverable error
            sleep_interval: gap between retries
        
        Returns:
            The new etag

        This API may create a new object or update an existing object.
        """
        return os_upload_json(
            self.os_client, 
            data,
            self.namespace_name,
            self.bucket_name,
            object_name,
            retry_count=retry_count,
            sleep_interval=sleep_interval,
        )

    def update_json(self, *, object_name:str, data:dict, etag:str, retry_count:float=5, sleep_interval:float=5) -> str:
        """Update an existing JSON object.

        Parameters:
            object_name: name of the object
            data: the json data
            etag: The object must match the etag. If you specify "*", then you can update the object regardless of the etag.
                  But API will fail if the object does not exist.
            retry_count: number of time to retry for recoverable error
            sleep_interval: gap between retries
        
        Returns:
            The new etag
        """
        assert etag is not None
        return os_upload_json(
            self.os_client, 
            data,
            self.namespace_name,
            self.bucket_name,
            object_name,
            retry_count=retry_count,
            sleep_interval=sleep_interval,
            opts={"if_match":etag}
        )

    def create_json(self, *, object_name:str, data:dict, retry_count:float=5, sleep_interval:float=5) -> str:
        """Create a new json objcet

        Parameter:
            object_name: name of the object
            data: the json data
            retry_count: number of time to retry for recoverable error
            sleep_interval: gap between retries
        
        Returns:
            The new etag
        """
        return os_upload_json(
            self.os_client, 
            data,
            self.namespace_name,
            self.bucket_name,
            object_name,
            retry_count=retry_count,
            sleep_interval=sleep_interval,
            opts={"if_none_match":"*"}
        )
    
    def transform_json(self, *, object_name:str, transformer:Callable[[dict], dict], collision_retry_count=5, collision_sleep_interval=5, retry_count=5, sleep_interval=5) -> Tuple[dict, dict]:
        """Transform a JSON object

        Parameter:
            object_name: name of the object
            transformer: a function to transform the object, it need to be a pure function.
            collision_retry_count: number of time to retry once collision encountered
            collision_sleep_interval: gap between collision retries
            retry_count: number of time to retry for recoverable error
            sleep_interval: gap between retries
        
        Returns:
            a tuple of 2 elements, first has the etag and data before the change, 2nd has the etag and data after the change
        """
        for i in range(0, collision_retry_count):
            data, etag = self.get_json(object_name=object_name, retry_count=retry_count, sleep_interval=sleep_interval)
            new_data = transformer(data)
            try:
                if etag is None:
                    assert data is None
                    new_etag = self.create_json(object_name=object_name, data=new_data, retry_count=retry_count, sleep_interval=sleep_interval)
                else:
                    new_etag = self.update_json(object_name=object_name, data=new_data, etag=etag, retry_count=retry_count, sleep_interval=sleep_interval)
                return {"etag": etag, "data": data}, {"etag": new_etag, "data": new_data}
            except oci.exceptions.ServiceError as e:
                if e.status != 412:
                    if etag is None:
                        logger.error(f"transform_json: failed to create json, namespace={self.namespace_name}, bucket={self.bucket_name}, object_name={object_name}, error={e}, message={str(e)}")
                    else:
                        logger.error(f"transform_json: failed to update json, namespace={self.namespace_name}, bucket={self.bucket_name}, object_name={object_name}, error={e}, message={str(e)}")
                    raise

                if i >= collision_retry_count - 1:
                    logger.error(f"transform_json: collision, (namespace={self.namespace_name}, bucket={self.bucket_name}, object_name={object_name}) failed for {i+1} times, error is: {e}, message is: {str(e)}, no more retrying...")
                    raise

                logger.warning(f"transform_json: collision, (namespace={self.namespace_name}, bucket={self.bucket_name}, object_name={object_name}) failed for {i+1} times, error is: {e}, message is: {str(e)}, retrying after {collision_sleep_interval} seconds...")
                time.sleep(collision_sleep_interval)


    def download_object(self, *, object_name:str, local_filename:str, retry_count:float=5, sleep_interval:float=5) -> str:
        """Download an object

        Parameter:
            object_name: name of the object
            local_filename: name of the file to hold download data
            retry_count: number of time to retry for recoverable error
            sleep_interval: gap between retries
        Returns:
            The new etag
        """
        return os_download(
            self.os_client,
            local_filename,
            self.namespace_name,
            self.bucket_name,
            object_name,
            retry_count=retry_count,
            sleep_interval=sleep_interval
        )

    def create_or_update_object(self, *, local_filename:str, object_name:str, retry_count:float=5, sleep_interval:float=5) -> str:
        """Create a new object or update an existing object.

        Parameters:
            local_filename: name of the file to be uploaded
            object_name: name of the object
            retry_count: number of time to retry for recoverable error
            sleep_interval: gap between retries
        
        Returns:
            The new etag

        This API may create a new object or update an existing object.
        """
        return os_upload(
            self.os_client, 
            local_filename,
            self.namespace_name,
            self.bucket_name,
            object_name,
            retry_count=retry_count,
            sleep_interval=sleep_interval
        )

    def upload_dir(self, *, local_dir_base:str, object_name_base:str, retry_count:float=5, sleep_interval:float=5) ->None:
        """Upload a directory or a file to the bucket recursively. Object is either created if not exist or updated if already exist.

        Parameters:
            local_dir_base:   the directory to upload.
            object_name_base: the destination of the upload location
            retry_count: number of time to retry for recoverable error
            sleep_interval: gap between retries
        
        Returns:
            None
        
        Anything other than file or dir will be ignored from local_dir_base
        """

        if os.path.isfile(local_dir_base):
            self.create_or_update_object(
                local_filename=local_dir_base, 
                object_name=object_name_base, 
                retry_count=retry_count, 
                sleep_interval=sleep_interval
            )
            return
        
        if os.path.isdir(local_dir_base):
            for f in os.listdir(local_dir_base):
                self.upload_dir(
                    local_dir_base=os.path.join(local_dir_base, f), 
                    object_name_base=os.path.join(object_name_base, f),
                    retry_count=retry_count, 
                    sleep_interval=sleep_interval
                )

    def copy_object(self, *, object_name:str, dst_bucket:'Bucket', dst_object_name:str, retry_count:float=5, sleep_interval:float=5) -> None:
        """Copy an object to another bucket.

        Parameters:
            object_name: the source object name
            dst_bucket: the destination bucket object
            dst_object_name: the destination object name
            retry_count: number of time to retry for recoverable error
            sleep_interval: gap between retries
        
        Returns:
            None
        """
        os_copy_object(
            self.os_client,
            self.namespace_name, self.bucket_name, object_name,
            dst_bucket.region, dst_bucket.namespace_name, dst_bucket.bucket_name, dst_object_name,
            retry_count=retry_count, 
            sleep_interval=sleep_interval
        )
