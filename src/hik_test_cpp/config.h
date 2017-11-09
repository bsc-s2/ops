#include <iostream>
#include <fstream>
#include <map>
#include <string>
#include <vector>

using namespace std;

std::vector<std::string> split_str(const std::string& str, const std::string& token=" ")
{
    typedef typename std::string::size_type size_type;
    size_type sl = token.length();
    std::vector<std::string> res;
    size_type pos=0;
    for(;;)
    {
        size_type newpos = str.find(token, pos);
        if(newpos == string::npos)
        {
            if(pos!=str.length())
            {
                res.push_back(str.substr(pos));
            }
            break;
        }
        else
        {
            if(newpos!=pos)
            {
                res.push_back(str.substr(pos, newpos-pos));
            }
        }
        pos = newpos+sl;
    }
    return res;
}

class Config
{
    map<string, string> m_config;
public:
    Config(string file)
    {
        parse_config(file);
        cout << "operator_type:" << is_download() << " thread_count:" << get_thread_count()
             << " test_time:" << get_test_time() << " file_size:" << get_file_size()
             << " file_name:" << get_file_name() << " open_log:" << open_log() << endl;
    }
    int parse_config(string& file)
    {
        ifstream ifs(file.c_str());
        if(!ifs)
            return -1;

        while(!ifs.eof())
        {
            string s;
            ifs >> s;
            if(s[0] == '#')
                continue;
            cout << s << endl;
            vector<string> s_vec = split_str(s, ",");
            for(auto itr = s_vec.begin(); itr != s_vec.end(); ++itr)
            {
                vector<string> info_vec = split_str(*itr, ":");
                if(info_vec.size() < 2)
                    continue;
                m_config[info_vec[0]] = info_vec[1];
            }
        }
        return 0;
    }

    bool is_download()
    {
        auto itr = m_config.find("operator_type");
        if(itr == m_config.end())
            return false;

        return (itr->second == "download");
    }

    bool is_download_prepare()
    {
        auto itr = m_config.find("operator_type");
        if(itr == m_config.end())
            return false;

        return (itr->second == "download_prepare");
    }

    int get_thread_count()
    {
        auto itr = m_config.find("thread_count");
        if(itr == m_config.end())
            return 1;

        return atoi(itr->second.c_str());
    }

    int get_test_time()
    {
        auto itr = m_config.find("test_time");
        if(itr == m_config.end())
            return 1;

        return atoi(itr->second.c_str());
    }

    int get_file_size()
    {
        auto itr = m_config.find("file_size");
        if(itr == m_config.end())
            return 1;

        return atoi(itr->second.c_str());
    }

    string get_file_name()
    {
        auto itr = m_config.find("file_name");
        if(itr == m_config.end())
            return "";

        return itr->second;
    }

    bool open_log()
    {
        auto itr = m_config.find("open_log");
        if(itr == m_config.end())
            return false;

        return (itr->second == "true");
    }

    bool save_download_file()
    {
        auto itr = m_config.find("save_download_file");
        if(itr == m_config.end())
            return false;

        return (itr->second == "true");

    }

    int get_max_rps()
    {
        auto itr = m_config.find("max_rps");
        if(itr == m_config.end())
            return 1500;

        return atoi(itr->second.c_str());
    }

    const char* get_bucket()
    {
        auto itr = m_config.find("bucket");
        if(itr == m_config.end())
            return "no such bucket";

        return itr->second.c_str();
    }

    const char* get_access_key()
    {
        auto itr = m_config.find("access_key");
        if(itr == m_config.end())
            return "no such access key";

        return itr->second.c_str();
    }

    const char* get_secret_key()
    {
        auto itr = m_config.find("secret_key");
        if(itr == m_config.end())
            return "no such secret key";

        return itr->second.c_str();
    }

    const char* get_addr()
    {
        auto itr = m_config.find("addr");
        if(itr == m_config.end())
            return "not found addr";

        return itr->second.c_str();
    }
};

