import ballerina/persist as _;

type Message record {|
    readonly string id;
    string url;
    string replyUrl;
    string state;
    string nodeId;
    int createdAt;
    byte[] headers;
    byte[] payload;
    string contentType;
    string method;
    Response? response;
|};

type Response record {|
    readonly string id;
    int statusCode;
    byte[] headers;
    byte[] payload;
    string contentType;
    Message message;
|};
