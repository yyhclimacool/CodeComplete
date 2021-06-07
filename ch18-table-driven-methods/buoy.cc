#include <iostream>
#include <fstream>
#include <string>
#include <unordered_map>
#include <vector>
#include <map>
#include <memory>

#define LOG(...) \
    do { \
        fprintf(stderr, "%s:%d ", __FILE__, __LINE__); \
        fprintf(stderr, __VA_ARGS__); \
        fprintf(stderr, "\n"); \
    }while(0);

enum class FieldType : size_t {
    TYPE_INT,
    TYPE_BOOL,
    TYPE_DOUBLE,
    TYPE_STRING,
    TYPE_TIMESTAMP,
};

static const std::unordered_map<std::string, FieldType> type_from_name = {
    {"int", FieldType::TYPE_INT}, 
    {"bool", FieldType::TYPE_BOOL},
    {"double", FieldType::TYPE_DOUBLE},
    {"string", FieldType::TYPE_STRING},
    {"ts", FieldType::TYPE_TIMESTAMP}
};

int ProcessMessageField(const std::string &msg, FieldType type) {
    switch(type) {
        case FieldType::TYPE_INT: {
            int ival = atol(msg.c_str());
            std::cout << ival << ",";
            break;
        }
        case FieldType::TYPE_BOOL: {
            bool b = static_cast<bool>(atol(msg.c_str()));
            std::cout << std::boolalpha << b << std::noboolalpha << ",";
            break;
        }
        case FieldType::TYPE_DOUBLE: {
            double d = std::strtod(msg.c_str(), NULL);
            std::cout << d << ",";
            break;
        }
        case FieldType::TYPE_STRING: {
            std::cout << msg << ",";
            break;
        }
        case FieldType::TYPE_TIMESTAMP: {
            unsigned long long ts = std::strtoull(msg.c_str(), NULL, 10);
            std::cout << "parsed_ts=" << ts << ",";
            break;
        }
        default: {
            LOG("field type not supported.");
            return -1;
            break;
        }
    }
    std::cout << std::endl;
    return 0;
}

// 用一个结构体表示每一种消息类型
// 每一种消息类型可以通过配置文件配置
// 如果消息类型有改变，比如有字段的增加
// 那么程序自动兼容，该结构体会从配置中读取增加的字段
struct MessagePattern { 
    size_t _num_fields;
    int _msg_id;
    std::string _msg_name;
    std::vector<std::pair<std::string, FieldType>> _msg_fields;

    // 解析一行的配置
    int readFromLine(const std::string &line) {
        std::vector<std::string> fields;
        boost::split(fields, line, boost::is_any_of(";"), boost::token_compress_on);
        if (fields.size() <= 2) {
            LOG("parse message pattern failed, origin_definition=%s", line);
            return -1;
        }
        _num_fields = fields.size();
        _msg_id = std::strtod(fields[0].c_str(), NULL);
        _msg_name = std::move(fields[1]);
        for (size_t i = 2; i < fields.size(); ++i) {
            auto middle_pos = fields[i].find(':');
            auto name = fields[i].substr(0, middle_pos);
            auto type = fields[i].substr(middle_pos + 1, fields[i].size() - middle_pos);
            LOG("field_name=%s,field_type=%s", name, type);
            auto it = type_from_name.find(type);
            if (it == type_from_name.end()) {
                LOG("field_type=%s not defined,parse message pattern failed, origin_definition=%s", type, line);
                return -1;
            }
            _msg_fields.emplace_back(std::move(name), it->second);
        }
        LOG("parse message definition success,origin_definition=%s,fields_num=%d", line, _num_fields);
        return 0;
    }

};

class MessagePatternManager {
public:
    MessagePatternManager(const std::string &file)
        : _message_pattern_definition_file(file) {}
    int Init() {
        LOG("start parsing message config file=%s", _message_pattern_definition_file);
        std::ifstream ifs(_message_pattern_definition_file);
        if (!ifs) {
            LOG("open file failed, filename=%s", _message_pattern_definition_file.c_str());
            return -1;
        }

        std::string line;
        while (getline(ifs, line)) {
            if (line.starts_with('#')) continue;

            MessagePattern mp;
            int ret = mp.readFromLine(line);
            if (ret != 0) {
                continue;
            }

            auto retit = _patterns.emplace(mp._msg_id, std::move(mp));
            if (retit.second) {
                LOG("message id already exists,ignoring it,origin_line=%s", line);
                continue;
            }
        }
        LOG("total patterns parsed num=%d", _patterns.size());
        return 0;
    }

    const MessagePattern *LocatePattern(int type_id) {
        auto it = _patterns.find(type_id);
        if (it == _patterns.end()) return nullptr;
        else return &(it->second);
    }
private:
    std::string _message_pattern_definition_file;
    std::map<int, MessagePattern> _patterns;
};

class MessageParser {
public:
    MessageParser() = default;
    void set_mpm(const std::shared_ptr<MessagePatternManager> &mpm) {
        _mpmsptr = mpm;
    }
    int Parse(const std::string file) {
        _message_file = file;
        std::string line;
        std::ifstream ifs(_message_file);
        if (!ifs) {
            LOG("open file=%s as file stream failed.", _message_file.c_str());
            return -1;
        }
        if (!_mpmsptr) {
            LOG("please set message pattern manager before run parse.");
            return -1;
        }
        // 读取每一行，判断是哪种消息类型，查询表，处理相应的类型
        while (getline(ifs, line)) {
            if (line.starts_with('#')) continue;
            std::vector<std::string> msg_fields;
            boost::split(msg_fields, line, boost::is_any_of(','), boost::token_compress_on);
            if (msg_fields.size() <= 1) {
                LOG("line=%s,it only has %d fields,supposed to ge 2,ignoring this line of message.", line.c_str(), msg_fields.size());
                continue;
            }
            int type_id = std::strtod(msg_fields[0].c_str(), NULL);
            const auto * mp = _mpmsptr->LocatePattern(type_id);
            if (!mp) {
                LOG("message definition not defined with type_id=%d, ignoring this line of message.", type_id);
                continue;
            }

            // 按照消息格式来处理这条消息
            // 实际消息内容要匹配消息pattern，没有值得地方要留空，满足这个条件下面的循环才是安全的
            // 当然要实现宽松的要求也是可以的
            size_t msg_idx = 0;
            for (; msg_idx < mp->_msg_fields.size(); ++msg_idx) {
                std::string thefield = msg_fields[1+msg_idx];
                auto ret = ProcessMessageField(thefield, mp->_msg_fields[msg_idx].second);
                if (ret != 0) {
                    LOG("ProcessMessageField failed for field=%s", thefield);
                    continue;
                }
            }
        }
        return 0;
    }
private:
    std::string _message_file;
    std::shared_ptr<MessagePatternManager> _mpmsptr;
};

int main(int argc, char **argv) {
    if (argc != 3) {
        std::cout << "Usage: " << argv[0] << " <message-definiton-file> <message-content-file>" << std::endl;
        return 0;
    }
    auto mpm = std::make_shared<MessagePatternManager>(argv[1]);
    auto ret = mpm->Init();
    if (ret != 0) return -1;
    MessageParser mp;
    mp.set_mpm(mpm);
    ret = mp.Parse(argv[2]);
    if (ret != 0) return -1;
}
