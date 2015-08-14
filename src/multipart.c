#include <string.h>
#include <stdlib.h>
#include "libs3.h"
#include "request.h"
#include "simplexml.h"

/** **************************************************************************
 * Multipart.c
 * 
 * This file is part of libs3.
 * 
 * libs3 is free software: you can redistribute it and/or modify it under the
 * terms of the GNU General Public License as published by the Free Software
 * Foundation, version 3 of the License.
 *
 * In addition, as a special exception, the copyright holders give
 * permission to link the code of this library and its programs with the
 * OpenSSL library, and distribute linked combinations including the two.
 *
 * libs3 is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License version 3
 * along with libs3, in a file named COPYING.  If not, see
 * <http://www.gnu.org/licenses/>.
 *
 ************************************************************************** **/



typedef struct InitialMultipartData
{
    SimpleXml simpleXml;
    int len;
    S3MultipartInitialHander *handler;
    string_buffer(upload_id, 128);
    void * userdata;
} InitialMultipartData;

static S3Status InitialMultipartCallback(int bufferSize, const char *buffer, void * callbackData) {

    InitialMultipartData * mdata = (InitialMultipartData*) callbackData;
    return simplexml_add(&(mdata->simpleXml), buffer, bufferSize);

}

static void InitialMultipartCompleteCallback(S3Status requestStatus, const S3ErrorDetails *s3ErrorDetails, void * callbackData) {
    InitialMultipartData * mdata = (InitialMultipartData*) callbackData;

    if (mdata->handler->responseHandler.completeCallback) {
        (*mdata->handler->responseHandler.completeCallback)(requestStatus, s3ErrorDetails, mdata->userdata);
    }

    if (mdata->handler->responseXmlCallback) {
        (*mdata->handler->responseXmlCallback)(mdata->upload_id, mdata->userdata);
    }    

    simplexml_deinitialize(&(mdata->simpleXml));
    free(mdata);
}    


static S3Status initialMultipartXmlCallback(const char *elementPath, const char *data, int dataLen,
                        void * callbackData) {
    InitialMultipartData *mdata = (InitialMultipartData* )callbackData;
    int fit;
    if (data) {
        if(!strcmp(elementPath, "InitiateMultipartUploadResult/UploadId")){
        string_buffer_append(mdata->upload_id,data, dataLen, fit);
    }
    }

    (void) fit;
    return S3StatusOK;
}

void S3_multipart_initial(S3BucketContext *bucketContext, const char *key, S3PutProperties *putProperties, S3MultipartInitialHander* handler, S3RequestContext * requestContext, void * callbackData) 
{

    InitialMultipartData *mdata = (InitialMultipartData* )malloc(sizeof(InitialMultipartData)); 
    simplexml_initialize(&(mdata->simpleXml), &initialMultipartXmlCallback, mdata);
    string_buffer_initialize(mdata->upload_id);
    mdata->handler= handler;
    mdata->userdata = callbackData;

    RequestParams params =
    {
        HttpRequestTypePOST,                           // httpRequestType
        { bucketContext->hostName,                    // hostName
      bucketContext->bucketName,                  // bucketName
          bucketContext->protocol,                    // protocol
          bucketContext->uriStyle,                  // uriStyle
          bucketContext->accessKeyId,                 // accessKeyId
          bucketContext->secretAccessKey },           // secretAccessKey
        key,                                            // key
        0,                                            // queryParams
        "uploads",                                   // subResource
        0,                                            // copySourceBucketName
        0,                                            // copySourceKey
        0,                                            // getConditions
        0,                                            // startByte
        0,                                            // byteCount
        putProperties,                                // putProperties
    handler->responseHandler.propertiesCallback,  // propertiesCallback
        0,                                            // toS3Callback
        0,                                            // toS3CallbackTotalSize
        InitialMultipartCallback,                     // fromS3Callback
        InitialMultipartCompleteCallback,             // completeCallback
        mdata                                         // callbackData
    };

    // Perform the request
    request_perform(&params, requestContext);
}


/*
 * S3 Upload Part
 */

void S3_multipart_upload_part(S3BucketContext *bucketContext, const char *key, S3PutProperties * putProperties, S3PutObjectHandler *handler, int seq, const char * upload_id, int partContentLength, S3RequestContext * requestContext, void* callbackData)
{

    char subResource[512];
    snprintf(subResource, 512, "partNumber=%d&uploadId=%s", seq, upload_id);

    RequestParams params =
    {
        HttpRequestTypePUT,                           // httpRequestType
        { bucketContext->hostName,                    // hostName
          bucketContext->bucketName,                  // bucketName
          bucketContext->protocol,                    // protocol
          bucketContext->uriStyle,                    // uriStyle
          bucketContext->accessKeyId,                 // accessKeyId
          bucketContext->secretAccessKey },           // secretAccessKey
        key,                                          // key
        0,                                            // queryParams
        subResource,                                  // subResource
        0,                                            // copySourceBucketName
        0,                                            // copySourceKey
        0,                                            // getConditions
        0,                                            // startByte
        0,                                            // byteCount
        putProperties,                                // putProperties
        handler->responseHandler.propertiesCallback,  // propertiesCallback
        handler->putObjectDataCallback,               // toS3Callback
        partContentLength,                            // toS3CallbackTotalSize
        0,                                            // fromS3Callback
        handler->responseHandler.completeCallback,    // completeCallback
        callbackData                                  // callbackData
    };

    request_perform(&params, requestContext);
}


/*
 * S3 commit multipart 
 *
 */

typedef struct CommitMultiPartData {
    SimpleXml simplexml;
    void * userdata;
    S3MultipartCommitHandler *handler;
    //response parsed from 
    string_buffer(location,128);
    string_buffer(etag,128);

} CommitMultiPartData;


static S3Status commitMultipartResponseXMLcallback(const char *elementPath, const char *data, int dataLen, void * callbackData) {
    int fit;
    CommitMultiPartData * commit_data = (CommitMultiPartData*) callbackData;
    if(data) {
        if(!strcmp(elementPath, "CompleteMultipartUploadResult/Location")) {
            string_buffer_append(commit_data->location, data, dataLen, fit);
        } else if (!strcmp(elementPath, "CompleteMultipartUploadResult/ETag")) {
            string_buffer_append(commit_data->etag, data, dataLen, fit);
        }
    }
    (void)fit;
    
    return S3StatusOK;
}


static S3Status commitMultipartCallback(int bufferSize, const char *buffer, void * callbackData) {
    CommitMultiPartData * data = (CommitMultiPartData*) callbackData;
    return simplexml_add(&(data->simplexml), buffer, bufferSize);
}


static S3Status commitMultipartPropertiesCallback
    (const S3ResponseProperties *responseProperties, void *callbackData)
{
    CommitMultiPartData *data = (CommitMultiPartData*) callbackData;
    
    if (data->handler->responseHandler.propertiesCallback)
        (*(data->handler->responseHandler.propertiesCallback))
            (responseProperties, data->userdata);
    return S3StatusOK;
}

static void commitMultipartCompleteCallback(S3Status requestStatus, const S3ErrorDetails *s3ErrorDetails, void * callbackData) {
    CommitMultiPartData *data = (CommitMultiPartData*) callbackData;
    if (data->handler->responseHandler.completeCallback)
        (*(data->handler->responseHandler.completeCallback))(requestStatus, s3ErrorDetails, data->userdata);
    if (data->handler->responseXmlCallback) {
        (*data->handler->responseXmlCallback)(data->location, data->etag, data->userdata);
    }
    simplexml_deinitialize(&(data->simplexml));
    free(data);
}


static int commitMultipartPutObject(int bufferSize, char *buffer, void *callbackData) 
{
    CommitMultiPartData *data = (CommitMultiPartData*) callbackData;
    if(data->handler->putObjectDataCallback)
        return data->handler->putObjectDataCallback(bufferSize, buffer, data->userdata);
    else
        return -1;
}

void S3_multipart_commit(S3BucketContext *bucketContext, const char *key, S3MultipartCommitHandler *handler, const char * upload_id, int contentLength, S3RequestContext * requestContext, void* callbackData) {
    char subResource[512];
    snprintf(subResource, 512, "uploadId=%s", upload_id);
    CommitMultiPartData * data = (CommitMultiPartData*)malloc(sizeof(CommitMultiPartData));
    data->userdata = callbackData;
    data->handler = handler;
    string_buffer_initialize(data->location);
    string_buffer_initialize(data->etag);

    simplexml_initialize(&(data->simplexml), commitMultipartResponseXMLcallback, data);

    RequestParams params =
    {
        HttpRequestTypePOST,                           // httpRequestType
        { bucketContext->hostName,                    // hostName
          bucketContext->bucketName,                  // bucketName
          bucketContext->protocol,                    // protocol
          bucketContext->uriStyle,                    // uriStyle
          bucketContext->accessKeyId,                 // accessKeyId
          bucketContext->secretAccessKey },           // secretAccessKey
        key,                                          // key
        0,                                            // queryParams
        subResource,                                  // subResource
        0,                                            // copySourceBucketName
        0,                                            // copySourceKey
        0,                                            // getConditions
        0,                                            // startByte
        0,                                            // byteCount
        0,                                            // putProperties
        commitMultipartPropertiesCallback,            // propertiesCallback
        commitMultipartPutObject,                     // toS3Callback
        contentLength,                                // toS3CallbackTotalSize
        commitMultipartCallback,                      // fromS3Callback
        commitMultipartCompleteCallback,              // completeCallback
        data                                          // callbackData
    };

    request_perform(&params, requestContext);

}
