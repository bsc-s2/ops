/*
   Copyright 2010-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
   This file is licensed under the Apache License, Version 2.0 (the "License").
   You may not use this file except in compliance with the License. A copy of
   the License is located at
http://aws.amazon.com/apache2.0/
This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.
*/
#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/Bucket.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/s3/model/DeleteBucketRequest.h>
#include <aws/s3/model/GetBucketAclRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/GetObjectRequest.h>

#include <fstream>
#include "thread_pool.h"
#include "config.h"

using namespace std;

pthread_mutex_t generate_key_mutex;
pthread_mutex_t generate_name_mutex;
pthread_mutex_t rps_mutex;
int succ_count = 0;
fstream fs_rps;
fstream fs_log;

Aws::S3::S3Client create_client(Aws::SDKOptions& options);
void upload_file(Aws::S3::S3Client s3_client, Aws::String file_path);
Aws::String get_upload_key();
void download_file(Aws::S3::S3Client s3_client, Aws::String file_path);
int expected_speed = 0;
Config cfg = Config("./config");

class CTaskUpload : public CTask
{
    Aws::String m_file;
    Aws::S3::S3Client m_cli;
    public:
        CTaskUpload(Aws::S3::S3Client client, Aws::String f): m_cli(client), m_file(f){}
        virtual ~CTaskUpload(){}
        virtual void update()
        {
            upload_file(m_cli, m_file);
        }
};

class CTaskDownload : public CTask
{
    Aws::S3::S3Client m_cli;
    Aws::String m_file;
    public:
        CTaskDownload(Aws::S3::S3Client client, Aws::String f): m_cli(client), m_file(f){}
        virtual ~CTaskDownload(){}
        virtual void update()
        {
            download_file(m_cli, m_file);
        }
};

string tm_to_str(const struct timeval& tv)
{
    struct tm* tblock = localtime(&tv.tv_sec);
    int ms = tv.tv_usec / 1000;
    string str;
    char buffer[1024] = {0};
    snprintf(buffer, 1024, "[%4d/%02d/%02d %02d:%02d:%02d:%04d]", tblock->tm_year + 1900,
             tblock->tm_mon + 1, tblock->tm_mday, tblock->tm_hour, tblock->tm_min,
             tblock->tm_sec, ms);
    str.append(buffer);
    return str;
}

void download_file(Aws::S3::S3Client s3_client, Aws::String file_path)
{
    struct timeval tv_begin;
    gettimeofday(&tv_begin, NULL);

    Aws::S3::Model::GetObjectRequest object_request;
    std::cout << "---------------------download file-------" << key << std::endl;
    object_request.WithBucket(cfg.get_bucket()).WithKey(file_path);

    auto get_object_outcome = s3_client.GetObject(object_request);
    if (get_object_outcome.IsSuccess())
    {
        struct timeval tv_end;
        gettimeofday(&tv_end, NULL);
        long ms = tv_end.tv_sec * 1000 + tv_end.tv_usec / 1000 -
             tv_begin.tv_sec * 1000 - tv_begin.tv_usec / 1000;

        pthread_mutex_lock(&rps_mutex);
        succ_count++;
        if(cfg.open_log()) fs_log << "begin_time:"<< tm_to_str(tv_begin)
                                  << " end_time:" << tm_to_str(tv_end)
                                  << " key:" << std::left <<  key
                                  << " used_ms:" << std::left << ms
                                  << " thread_id:" << (unsigned long int)pthread_self()
                                  << endl;
        pthread_mutex_unlock(&rps_mutex);

    }
    else
    {
        std::cout << "GetObject error: "
                  << get_object_outcome.GetError().GetExceptionName()
                  << " "
                  << get_object_outcome.GetError().GetMessage()
                  << std::endl;
    }
}

void random_uuid(char buf[37])
{
    const char *c = "89ab";
    char *p = buf;
    int n;
    for(n = 0; n < 16; ++n)
    {
        int b = rand() % 255;
        switch(n)
        {
            case 6:
                sprintf(p, "4%x", b % 15);
                break;
            case 8:
                sprintf(p, "%c%x", c[rand() % strlen(c)], b % 15);
                break;
            default:
                sprintf(p, "%02x", b);
                break;
        }

        p += 2;
        switch(n)
        {
            case 3:
            case 5:
            case 7:
            case 9:
                *p++ = '-';
                break;
        }
    }
    *p = 0;
}

void upload_file(Aws::S3::S3Client s3_client, Aws::String file_path)
{
    struct timeval tv_begin;
    gettimeofday(&tv_begin, NULL);
    std::cout << "---------------------upload file-------" << file_path << std::endl;
    Aws::S3::Model::PutObjectRequest object_request;
    Aws::String key = get_upload_key();

    object_request.WithBucket(cfg.get_bucket()).WithKey(key);
    ///Binary files must also have the std::ios_base::bin flag or'ed in
    auto input_data = Aws::MakeShared<Aws::FStream>("PutObjectInputStream",
                                    file_path.c_str(), std::ios_base::in);

    object_request.SetBody(input_data);
    auto put_object_outcome = s3_client.PutObject(object_request);
    if (put_object_outcome.IsSuccess())
    {
        struct timeval tv_end;
        gettimeofday(&tv_end, NULL);
        long ms = tv_end.tv_sec * 1000 + tv_end.tv_usec / 1000 -
             tv_begin.tv_sec * 1000 - tv_begin.tv_usec / 1000;

        pthread_mutex_lock(&rps_mutex);
        succ_count++;
        if(cfg.open_log()) fs_log << "begin_time:"<< tm_to_str(tv_begin)
                                  << " end_time:" << tm_to_str(tv_end)
                                  << " key:" << std::left << key
                                  << " used_ms:" << std::left << ms
                                  << " thread_id:" << (unsigned long int)pthread_self() << endl;
        pthread_mutex_unlock(&rps_mutex);
    }
    else
    {
        std::cout << "PutObject error: "
                  << put_object_outcome.GetError().GetExceptionName() << " "
                  << put_object_outcome.GetError().GetMessage() << std::endl;
    }
}

Aws::S3::S3Client create_client(Aws::SDKOptions& options)
{
    Aws::InitAPI(options);
    Aws::Client::ClientConfiguration config;
    config.region = Aws::Region::US_EAST_1;
    config.scheme = Aws::Http::Scheme::HTTP;

    Aws::String endpoint(cfg.get_addr());
    config.endpointOverride = endpoint;
    config.connectTimeoutMs = 300000;
    config.requestTimeoutMs = 300000;

    Aws::String access_key(cfg.get_access_key());
    Aws::String secret_key(cfg.get_secret_key());
    return Aws::S3::S3Client(Aws::Auth::AWSCredentials(access_key, secret_key), config);
}

Aws::String get_upload_key()
{
    static int id = 0;
    static char uuid[37] = {0};

    char buffer[1024] = {0};
    pthread_mutex_lock(&generate_key_mutex);
    if(id == 0)
    {
        random_uuid(uuid);
    }
    id++;
    sprintf(buffer, "key_%s_%d", uuid, id);
    pthread_mutex_unlock(&generate_key_mutex);

    return Aws::String(buffer);
}
void count_rps(struct timeval& begin, struct timeval& end)
{
    long ms_rps = end.tv_sec * 1000 + end.tv_usec / 1000 -
        begin.tv_sec * 1000 - begin.tv_usec / 1000;

    if(ms_rps < 1000)
    {
        return;
    }

    begin = end;

    pthread_mutex_lock(&rps_mutex);
    int count = succ_count;
    succ_count = 0;
    pthread_mutex_unlock(&rps_mutex);

    if(cfg.open_log()) fs_rps << tm_to_str(end) << " rps:" << count << "/s" << endl;
}

int main(int argc, char** argv)
{
    expected_speed = cfg.get_max_rps() / cfg.get_thread_count();
    if(expected_speed == 0)
        expected_speed = 1;

    ifstream fin(cfg.get_file_name().c_str());
    if (!fin && !cfg.is_download())
    {
       std::cout << "file don't exist!!!" << endl;
       return 0;
    }
    fin.close();

    fs_rps.open("./rps.log", ios::in | ios::out | ios::trunc);
    fs_log.open("./out.log", ios::in | ios::out | ios::trunc);

    pthread_mutex_init(&generate_key_mutex, NULL);
    pthread_mutex_init(&rps_mutex, NULL);
    pthread_mutex_init(&generate_name_mutex, NULL);

    Aws::SDKOptions options;
    Aws::S3::S3Client s3_client = create_client(options);

    CThreadPool pool(expected_speed);
    pool.Start(cfg.get_thread_count());

    struct timeval tv_begin;
    gettimeofday(&tv_begin, NULL);
    struct timeval tv_rps = tv_begin;
    struct timeval tv_rps_sec = tv_begin;

    for(;;)
    {
        struct timeval tv_now;
        gettimeofday(&tv_now, NULL);

        int task_count = pool.TaskCount();
        if(task_count < cfg.get_thread_count() * 2)
        {
            CTask* task= NULL;
            if(!cfg.is_download())
                task = new CTaskUpload(s3_client, cfg.get_file_name().c_str());
            else
                task = new CTaskDownload(s3_client, cf.get_file_name.c_str());

            pool.Put(task);
        }
        else
        {
            usleep(100);
        }

        count_rps(tv_rps_sec, tv_now);

        if(tv_now.tv_sec - tv_begin.tv_sec > cfg.get_test_time())
            break;
    }
    pool.Stop();

    fs_rps.close();
    fs_log.close();
    pthread_mutex_destroy(&generate_key_mutex);
    pthread_mutex_destroy(&generate_name_mutex);
    pthread_mutex_destroy(&rps_mutex);
    Aws::ShutdownAPI(options);
}
