// Copyright (c) 2023, WSO2 LLC. (https://www.wso2.com).
//
// WSO2 LLC. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

import ballerina/http;
import ballerina/log;
import ballerina/task;
import ballerina/time;
import ballerina/uuid;

import ballerina/lang.'string;
import ballerina/lang.value;

import ResilientProxy.db;

configurable string nodeId = uuid:createType1AsString();
configurable int:Unsigned16[] allowedResponseCodes = [200, 201, 202];
configurable int retentionPeriod = 60; //1*24*60*60; // 1 day in seconds.

final db:Client db = check new ();

listener http:Listener proxyEP = new (9090);

const X_URL = "X-Url";
const X_REPLY = "X-Reply";
const X_REPLY_METHOD = "X-Reply-Method";
const X_ACTIVITY = "X-Activity";

service on proxyEP {
    isolated resource function default submit(http:Request req) returns http:Accepted|http:BadRequest|http:InternalServerError {
        do {
            map<string> headersMap = map from var header in req.getHeaderNames()
                select [header, check req.getHeader(header)];
            // TODO : Handle Authentication and Authorization.

            if !headersMap.hasKey(X_URL) || !headersMap.hasKey(X_REPLY) || !headersMap.hasKey(X_REPLY_METHOD) {
                string message = string `Required headers not found. Required headers: "${X_URL}", "${X_REPLY}", "${X_REPLY_METHOD}".`;
                http:BadRequest badRequest = {body: {message}};
                return badRequest;
            }

            // Create Payload.
            string id = uuid:createType1AsString();
            string url = headersMap.get(X_URL);
            string replyUrl = headersMap.get(X_REPLY);
            string replyMethod = headersMap.get(X_REPLY_METHOD);
            _ = headersMap.remove(X_URL);
            _ = headersMap.remove(X_REPLY);
            _ = headersMap.remove(X_REPLY_METHOD);
            byte[] headers = headersMap.toJsonString().toBytes();
            byte[] payload = check req.getBinaryPayload();
            string contentType = req.getContentType();
            string method = req.method;
            int createdAt = time:utcNow()[0];

            db:MessageInsert message = {
                id,
                url,
                replyUrl,
                replyMethod,
                state: CREATED,
                nodeId,
                createdAt,
                headers,
                payload,
                contentType,
                method
            };
            // Persist request and accept.
            _ = check db->/messages.post([message]);
            http:Accepted accepted = {headers: {X_ACTIVITY: id}};
            return accepted;
        } on fail error e {
            string reference = uuid:createType1AsString();
            string message = "Error occurred while processing transaction. Retry again.";
            log:printError(message, e, reference = reference);
            http:InternalServerError err = {body: {message, reference}};
            return err;
        }
    }

    isolated resource function get message(string id) returns Status|http:NotFound {
        do {
            Status status = check db->/messages/[id];
            return status;
        } on fail error e {
            string reference = uuid:createType1AsString();
            string message = "Message ID not found";
            log:printError(message, e, reference = reference);
            return {body: {message, reference}};
        }
    }
}

public function main() returns error? {
    // Starting Scheduler.
    _ = check task:scheduleJobRecurByFrequency(new SendMessageJob(), 0.5, 1);
    _ = check task:scheduleJobRecurByFrequency(new ScheduleFailedMessageJob(), 5, 1);
    _ = check task:scheduleJobRecurByFrequency(new RetryFailedReplyJob(), 0.5, 1);
    _ = check task:scheduleJobRecurByFrequency(new CleanupJob(), 60, 1);
}

isolated class SendMessageJob {
    *task:Job;

    public function execute() {
        // Retrieve a message and schedule it for send.
        final db:Message? msg = scheduleMessage();
        if msg is () {
            // Nothing to send.
            return;
        }
        // Send the message.
        db:Response? sendMessageResult = sendMessage(msg);
        if sendMessageResult is () {
            // Error, return to retry.
            return;
        }
        // Send the response to the reply URL.
        sendReply(sendMessageResult, msg.replyUrl, msg.replyMethod);
    }
}

isolated class ScheduleFailedMessageJob {
    *task:Job;

    public function execute() {
        transaction {
            stream<db:Message, error?> messages = db->/messages();
            check from var message in messages
                where message.state == SENT_FAILED
                order by message.createdAt ascending
                do {
                    _ = check db->/messages/[message.id].put({state: SCHEDULED, nodeId});
                };
            _ = check commit;
        } on fail error e {
            log:printError("Error occurred while retrieving the message from DB", e);
        }
    }
}

isolated class RetryFailedReplyJob {
    *task:Job;

    public function execute() {
        transaction {
            stream<MessageWithResponse, error?> messages = db->/messages();
            MessageWithResponse[] messageList = check from var message in messages
                where message.state == REPLY_FAILED
                order by message.createdAt ascending
                limit 1
                select message;

            if messageList.length() == 0 {
                check commit;
                return;
            } else {
                MessageWithResponse msg = messageList[0];
                sendReply(msg.response, msg.replyUrl, msg.replyMethod);
                check commit;
                return;
            }
        } on fail error e {
            log:printError("Error occurred while retrieving failed message from DB", e);
        }
    }
}

isolated class CleanupJob {
    *task:Job;

    public function execute() {
        transaction {
            int currentTime = time:utcNow()[0];
            stream<db:Message, error?> messages = db->/messages();
            check from var message in messages
                where message.state == COMPLETED && retentionPeriod > currentTime - message.createdAt
                order by message.createdAt ascending
                do {
                    _ = check db->/messages/[message.id].delete();
                    _ = check db->/responses/[message.id].delete();
                };
            _ = check commit;
        } on fail error e {
            log:printError("Error occurred while retrieving the message from DB", e);
        }
    }
}

isolated function scheduleMessage() returns db:Message? {
    transaction {
        stream<db:Message, error?> messages = db->/messages();
        db:Message[] messageList = check from var message in messages
            where message.state == CREATED || message.state == SCHEDULED
            order by message.createdAt ascending
            limit 1
            select message;
        if messageList.length() == 0 {
            _ = check commit;
            return;
        } else {
            db:Message msg = messageList[0];
            _ = check db->/messages/[msg.id].put({state: SCHEDULED, nodeId});
            _ = check commit;
            return msg;
        }
    } on fail error e {
        log:printError("Error occurred while retrieving the message from DB", e);
    }
    return;
}

isolated function sendMessage(db:Message msg) returns db:Response? {
    do {
        http:Client ep = check new (msg.url);
        http:Request request = check createHttpRequest(msg, msg.method);
        http:Response|error response = ep->execute(msg.method, ".", request);
        if response is error {
            _ = check db->/messages/[msg.id].put({state: SENT_FAILED});
            fail response;
        }

        final int statusCode = response.statusCode;
        if allowedResponseCodes.some(i => i == statusCode) {
            log:printDebug("Message sent", message = msg.id);
            // Update the state and response
            transaction {
                string id = uuid:createType1AsString();
                map<string> headersMap = map from string header in response.getHeaderNames()
                    select [header, check response.getHeader(header)];
                byte[] headers = headersMap.toJsonString().toBytes();
                byte[] payload = check response.getBinaryPayload();
                string contentType = response.getContentType();
                db:Response res = {id, headers, payload, contentType, statusCode, responseId: msg.id};
                _ = check db->/responses.post([res]);
                _ = check db->/messages/[msg.id].put({state: SENT});
                check commit;
                return res;
            }
        } else {
            _ = check db->/messages/[msg.id].put({state: SENT_FAILED});
            log:printError("Error occurred while sending the message to the URL", reason = "Status code mismatch", code = response.statusCode, message = msg.id);
        }
    } on fail error e {
        log:printError("Error occurred while sending the message to the URL", e, message = msg.id);
    }
    return;
}

isolated function sendReply(db:Response res, string replyUrl, string replyMethod) {
    do {
        http:Client ep = check new (replyUrl);
        http:Request request = check createHttpRequest(res, replyMethod);
        http:Response|error response = ep->execute(replyMethod, ".", request);
        if response is error {
            _ = check db->/messages/[res.responseId].put({state: REPLY_FAILED});
            fail response;
        }
        final int statusCode = response.statusCode;
        if allowedResponseCodes.some(i => i == statusCode) {
            log:printDebug("Message Complete", message = res.responseId);
            _ = check db->/messages/[res.responseId].put({state: COMPLETED});
        } else {
            _ = check db->/messages/[res.responseId].put({state: REPLY_FAILED});
            log:printError("Error occurred while sending the reply to the URL", reason = "Status code mismatch", code = response.statusCode);
        }
    } on fail error e {
        log:printError("Error occurred while sending the reply to the URL", e, message = res.responseId);
    }
}

# State of the activity.
enum State {
    CREATED,
    SCHEDULED,
    SENT,
    SENT_FAILED,
    REPLY_FAILED,
    COMPLETED
}

# Represents the activity state
type Status record {|
    # Activity ID.
    string id;
    # State of the transaction.
    State state;
|};

# Represents a network message, which is a request or a response.
type NetworkMessage record {
    # Headers of the message in serialized form.
    byte[] headers;
    # Payload of the message in serialized form.
    byte[] payload;
    # Content type of the payload.
    string contentType;
};

type MessageWithResponse record {|
    *db:Message;
    db:Response response;
|};

isolated function createHttpRequest(NetworkMessage msg, string method) returns http:Request|error {
    map<string> reqHeaders = check (check value:fromJsonString(check string:fromBytes(msg.headers))).cloneWithType();

    http:Request request = new ();
    foreach [string, string] kv in reqHeaders.entries() {
        request.setHeader(kv[0], kv[1]);
    }
    request.setBinaryPayload(msg.payload);
    check request.setContentType(msg.contentType);
    request.method = method;

    return request;
}
