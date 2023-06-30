import ballerina/persist as _;

type Message record {|
    readonly string id;
    string url;
    string replyUrl;
    string replyMethod;
    string state;
    string nodeId;
    int createdAt;
    byte[] headers;
    byte[] payload;
    string contentType;
    string method;
    Response? response; 
    // TODO : Clarify, Foreign key creation seems not developer friendly. 
|};

type Response record {|
    readonly string id;
    int statusCode;
    byte[] headers;
    byte[] payload;
    string contentType;
    Message message;
|};
