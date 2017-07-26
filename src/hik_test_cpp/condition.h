#include <iostream>
#include <time.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

using namespace std;

class condition_impl
{
    public:
        condition_impl(void)
        {
            pthread_cond_init(&cond, 0);
            pthread_mutex_init(&mutex, 0);
        }

        ~condition_impl(void)
        {
            pthread_cond_destroy(&cond);
            pthread_mutex_destroy(&mutex);
        }

        void wait(void)
        {
            pthread_cond_wait(&cond, &mutex);
        }

        bool timed_wait(int ms_time)
        {
            timespec ts;
            ts.tv_sec = time(NULL) + ms_time / 1000;
            ts.tv_nsec = (ms_time % 1000) * 1000 * 1000;
            int ret = pthread_cond_timedwait(&cond, &mutex, &ts);
            if(ret != ETIMEDOUT)
                return true;
            else
                return false;
        }

        void notify_one(void)
        {
            pthread_cond_signal(&cond);
        }

        void notify_all(void)
        {
            pthread_cond_broadcast(&cond);
        }

        void do_lock(void)
        {
            pthread_mutex_lock(&mutex);
        }

        void do_unlock(void)
        {
            pthread_mutex_unlock(&mutex);
        }

    private:
        pthread_cond_t cond;
        pthread_mutex_t mutex;
};

