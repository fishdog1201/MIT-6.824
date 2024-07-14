#include <iostream>
#include <vector>
#include <string>
#include <string.h>

using namespace std;

struct KeyValue {
    string key;
    string value;
};

vector<string> split(char* text, size_t len)
{
    vector<string> str;
    string tmp = "";

    for (size_t i = 0; i < len; i++) {
        if (isalpha(text[i])) {
            tmp += text[i];
        } else {
            str.emplace_back(tmp);
            tmp = "";
        }
    }

    if (tmp.size() != 0) {
        str.emplace_back(tmp);
    }

    return str;
}

extern "C"
vector<KeyValue> mapF(KeyValue& kv) {
    vector<KeyValue> kvs;
    int len = kv.value.size();
    char content[len + 1];
    strcpy(content, kv.value.c_str());
    vector<string> str = split(content, len);
    for (const auto& s: str) {
        KeyValue tmp;
        tmp.key = s;
        tmp.value = "1";
        kvs.emplace_back(tmp);
    }
    return kvs;
}

extern "C"
vector<string> reduceF(vector<KeyValue> kvs) {
    vector<string> str;
    for (const auto& kv: kvs) {
        str.emplace_back(to_string(kv.value.size()));
    }
    return str;
}