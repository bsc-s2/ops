#include <iostream>
#include <time.h>
#include <pthread.h>
#include <ctime>
#include <errno.h>
#include <vector>
#include <string>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include "active_queue.h"

using namespace std;

class CTask
{
    public:
        CTask(){}
        virtual ~CTask(){}
        virtual void update()
        {
        }
};

class CThreadPool;
class CThread
{
    public:
        CThread(CThreadPool* pool, int limit_speed): m_pool(pool),
                                                     m_stop(false),
                                                     m_limit_speed(limit_speed)
        {
        }
        virtual ~CThread()
        {
        }

    public:
        void Stop();
        void Run();
        void Start();
        pthread_t GetTid(){ return m_tid;}

    protected:
        CThreadPool* m_pool;
        bool m_stop;
        pthread_t m_tid;
        int m_limit_speed;
};

class CThreadPool
{
    int m_limit_speed;
    public:
        CThreadPool(int limit_speed): m_limit_speed(limit_speed){}
        virtual ~CThreadPool(){}

    public:
        void Start(int thread_num)
        {
            for(int i = 0; i < thread_num; i++)
            {
                CThread* th = new CThread(this, m_limit_speed);
                th->Start();
                m_threads.push_back(th);
            }
        }
        void Stop()
        {
            for(size_t i = 0; i < m_threads.size(); i++)
            {
                m_threads[i]->Stop();
            }
            m_queue.stop();
            for(size_t i = 0; i < m_threads.size(); i++)
            {
                pthread_join(m_threads[i]->GetTid(), NULL);
                delete m_threads[i];
            }
            m_threads.clear();
        }
        void Put(CTask* task)
        {
            m_queue.put(task);
        }

        CTask* Get()
        {
            CTask* task = NULL;
            m_queue.get(task, -1);
            return task;
        }

        void Done(CTask* task)
        {
            delete task;
        }

        size_t TaskCount()
        {
            return m_queue.size();
        }

        void ClearTask()
        {
            m_queue.clear();
        }

    protected:
        active_queue<CTask*> m_queue;
        std::vector<CThread*> m_threads;
};

void CThread::Stop()
{
    m_stop = true;
}
void CThread::Run()
{
    struct timeval tv;
    gettimeofday(&tv, NULL);
    long long sum = 0;
    while(!m_stop)
    {
        CTask* task = m_pool->Get();
        if(task == NULL)
            continue;
        task->update();
        m_pool->Done(task);

        // limit speed in every thread
        sum++;
        struct timeval tv_now;
        gettimeofday(&tv_now, NULL);
        long long  used_us = (tv_now.tv_sec - tv.tv_sec) * 1000000 +
                             tv_now.tv_usec - tv.tv_usec;

        long long expected_us = sum * 1000000 / m_limit_speed;
        //cout << "exp_us:" << expected_us << " used_us:" << used_us << endl;
        if(used_us < expected_us)
        {
            usleep(expected_us - used_us);
        }
    }
}

void* svc(void* param)
{
    ((CThread*)param)->Run();
    return NULL;
}

void CThread::Start()
{
    int ret = pthread_create(&m_tid, NULL, svc, (void*)this);
    if(ret != 0)
    {
        cout << "create thread failed!!!!" << endl;
    }
}
