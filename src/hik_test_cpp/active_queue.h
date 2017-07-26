#include <iostream>
#include <list>

#include "condition.h"

using namespace std;

template<class T>
class active_queue
{
    public:
        active_queue(){}
        ~active_queue(){}

        bool put(const T& item);
        bool get(T& item, int timeout_ms = -1);
        void stop();
        void clear();
        size_t size()
        {
            m_con.do_lock();
            size_t s = m_queue.size();
            m_con.do_unlock();

            return s;
        }

    private:
        typedef list<T> queue_type;

        queue_type  m_queue;
        condition_impl m_con;
};

    template<class T>
bool active_queue<T>::put(const T& item)
{
    m_con.do_lock();
    m_queue.push_back(item);
    m_con.notify_one();
    m_con.do_unlock();
    return true;
}

    template<class T>
bool active_queue<T>::get(T& item, int timeout_ms)
{
    m_con.do_lock();
    if(m_queue.empty())
    {
        m_con.wait();
    }

    if(!m_queue.empty())
    {
        item = m_queue.front();
        m_queue.pop_front();
    }
    m_con.do_unlock();
    return true;
}

    template<class T>
void active_queue<T>::stop()
{
    m_con.do_lock();
    for(auto itr = m_queue.begin(); itr != m_queue.end(); ++itr)
        delete *itr;
    m_queue.clear();
    m_con.notify_all();
    m_con.do_unlock();
}

    template<class T>
void active_queue<T>::clear()
{
    m_con.do_lock();
    for(auto itr = m_queue.begin(); itr != m_queue.end(); ++itr)
        delete *itr;
    m_queue.clear();
    m_con.do_unlock();
}

