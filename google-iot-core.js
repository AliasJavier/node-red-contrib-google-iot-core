/**
 * Copyright JS Foundation and other contributors, http://js.foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 * Modifications Copyright 2017 Sense Tecnic Systems, Inc.
 **/

module.exports = function (RED) {
    "use strict";
    var mqtt = require("mqtt");
    var util = require("util");
    var isUtf8 = require('is-utf8');
    var jwt = require('jsonwebtoken');
    var path = require("path");
    var fs = require("fs-extra");    
    var MQTTStore = require('mqtt-nedb-store');
    
    var mqttDir = path.join(RED.settings.userDir, 'google-iot-core');

    // create a directory if needed for the data
    fs.ensureDirSync(mqttDir);

    function matchTopic(ts, t) {
        if (ts == "#") {
            return true;
        }
        var re = new RegExp("^" + ts.replace(/([\[\]\?\(\)\\\\$\^\*\.|])/g, "\\$1").replace(/\+/g, "[^/]+").replace(/\/#$/, "(\/.*)?") + "$");
        return re.test(t);
    }

    // Create a Cloud IoT Core JWT for the given project id, signed with the given
    // private key.
    function createJwt (projectId, privateKey) {
        // Create a JWT to authenticate this device. The device will be disconnected
        // after the token expires, and will have to reconnect with a new token. The
        // audience field should always be set to the GCP project id.
        const token = {
            'iat': Math.round(Date.now() / 1000),
            'exp': Math.round(Date.now() / 1000) + 60 * 60,  // 60 minutes
            'aud': projectId
        };
        return jwt.sign(token, privateKey, { algorithm: 'RS256' });
    }

    function MQTTBrokerNode(n) {

        RED.nodes.createNode(this, n);
        this.manager = MQTTStore(path.join(mqttDir,  n.id));

        // Configuration options passed by Node Red
        this.broker = n.broker;
        this.port = n.port;
        this.clientid = n.clientid;
        this.projectid = n.projectid;
        this.deviceid = n.deviceid;
        this.verifyservercert = n.verifyservercert;
        this.keepalive = n.keepalive;
        this.cleansession = n.cleansession;
        this.compactinterval = n.compactinterval;
        this.persistin = n.persistin;
        this.persistout = n.persistout;

        // Config node state
        this.brokerurl = "";
        this.connected = false;
        this.connecting = false;
        this.closing = false;
        this.options = {};
        this.queue = [];
        this.subscriptions = {};

        if (n.birthTopic) {
            this.birthMessage = {
                topic: n.birthTopic,
                payload: n.birthPayload || "",
                qos: Number(n.birthQos || 0),
                retain: n.birthRetain == "true" || n.birthRetain === true
            };
        }

        if (typeof this.verifyservercert === 'undefined') {
            this.verifyservercert = false;
        }
        if (typeof this.keepalive === 'undefined') {
            this.keepalive = 60;
        } else if (typeof this.keepalive === 'string') {
            this.keepalive = Number(this.keepalive);
        }
        if (typeof this.cleansession === 'undefined') {
            this.cleansession = true;
        }

        // Create the URL to pass in to the MQTT.js library
        if (this.brokerurl === "") {
            this.brokerurl = "mqtts://";
            if (this.broker !== "") {
                this.brokerurl = this.brokerurl + this.broker + ":" + this.port;
            } else {
                this.brokerurl = this.brokerurl + "localhost:1883";
            }
        }

        if (!this.clientid) {
            this.warn(RED._("google-iot-core.errors.missingclientid"));
        }

        // Build options for passing to the MQTT.js API
        this.options.clientId = this.clientid;
        this.options.username = 'unused';
        this.options.keepalive = this.keepalive;
        this.options.clean = this.cleansession;
        this.options.reconnectPeriod = RED.settings.mqttReconnectTime || 5000;
        this.options.connectTimeout = 30000;

        if (n.tls) {
            var tlsNode = RED.nodes.getNode(n.tls);
            if (tlsNode) {
                tlsNode.addTLSOptions(this.options);
            }
        }
        this.options.password = createJwt(this.projectid, this.options.key);

        // configure db storage
        if (this.persistin) {
            this.options.incomingStore = this.manager.incoming;
            this.manager.incoming.db.persistence.setAutocompactionInterval(this.compactinterval*1000);
        }
        if (this.persistout) {
            this.options.outgoingStore = this.manager.outgoing;
            this.manager.outgoing.db.persistence.setAutocompactionInterval(this.compactinterval*1000);            
        }

        // If there's no rejectUnauthorized already, then this could be an
        // old config where this option was provided on the broker node and
        // not the tls node
        if (typeof this.options.rejectUnauthorized === 'undefined') {
            this.options.rejectUnauthorized = (this.verifyservercert == "true" || this.verifyservercert === true);
        }

        if (n.willTopic) {
            this.options.will = {
                topic: n.willTopic,
                payload: n.willPayload || "",
                qos: Number(n.willQos || 0),
                retain: n.willRetain == "true" || n.willRetain === true
            };
        }

        // Define functions called by MQTT in and out nodes
        var node = this;
        this.users = {};

        this.register = function (mqttNode) {
            node.users[mqttNode.id] = mqttNode;
            if (Object.keys(node.users).length === 1) {
                node.connect();
            }
        };

        this.deregister = function (mqttNode, done) {
            delete node.users[mqttNode.id];
            if (node.closing) {
                return done();
            }
            if (Object.keys(node.users).length === 0) {
                if (node.client && node.client.connected) {
                    return node.client.end(done);
                } else {
                    node.client.end();
                    return done();
                }
            }
            done();
        };

        this.connect = function () {
            if (!node.connected && !node.connecting) {
                node.connecting = true;
                try {
                    node.client = mqtt.connect(node.brokerurl, node.options);
                    node.client.setMaxListeners(0);
                    // Register successful connect or reconnect handler
                    node.client.on('connect', function () {
                        node.connecting = false;
                        node.connected = true;
                        node.log(RED._("google-iot-core.state.connected", { broker: (node.clientid ? node.clientid + "@" : "") + node.brokerurl }));
                        for (var id in node.users) {
                            if (node.users.hasOwnProperty(id)) {
                                node.users[id].status({ fill: "green", shape: "dot", text: "node-red:common.status.connected" });
                            }
                        }
                        // Remove any existing listeners before resubscribing to avoid duplicates in the event of a re-connection
                        node.client.removeAllListeners('message');

                        // Re-subscribe to stored topics
                        for (var s in node.subscriptions) {
                            if (node.subscriptions.hasOwnProperty(s)) {
                                var topic = s;
                                var qos = 0;
                                for (var r in node.subscriptions[s]) {
                                    if (node.subscriptions[s].hasOwnProperty(r)) {
                                        qos = Math.max(qos, node.subscriptions[s][r].qos);
                                        node.client.on('message', node.subscriptions[s][r].handler);
                                    }
                                }
                                var options = { qos: qos };
                                node.client.subscribe(topic, options);
                            }
                        }

                        // Send any birth message
                        if (node.birthMessage) {
                            node.publish(node.birthMessage);
                        }
                    });
                    node.client.on("reconnect", function () {
                        for (var id in node.users) {
                            if (node.users.hasOwnProperty(id)) {
                                node.users[id].status({ fill: "yellow", shape: "ring", text: "node-red:common.status.connecting" });
                            }
                        }
                    })
                    // Register disconnect handlers
                    node.client.on('close', function () {
                        // refresh JWT token
                        node.client.options.password = createJwt(node.projectid, node.options.key);
                        if (node.connected) {
                            node.connected = false;
                            node.log(RED._("google-iot-core.state.disconnected", { broker: (node.clientid ? node.clientid + "@" : "") + node.brokerurl }));
                            for (var id in node.users) {
                                if (node.users.hasOwnProperty(id)) {
                                    node.users[id].status({ fill: "red", shape: "ring", text: "node-red:common.status.disconnected" });
                                }
                            }
                        } else if (node.connecting) {
                            node.log(RED._("google-iot-core.state.connect-failed", { broker: (node.clientid ? node.clientid + "@" : "") + node.brokerurl }));
                        }
                    });

                    // The client's own reconnect logic will take care of errors
                    node.client.on('error', function (error) {});
                }catch(err) {
                    console.log(err);
                }
            }
        };

        this.subscribe = function (topic, qos, callback, ref) {
            ref = ref || 0;
            node.subscriptions[topic] = node.subscriptions[topic] || {};
            var sub = {
                topic: topic,
                qos: qos,
                handler: function (mtopic, mpayload, mpacket) {
                    if (matchTopic(topic, mtopic)) {
                        callback(mtopic, mpayload, mpacket);
                    }
                },
                ref: ref
            };
            node.subscriptions[topic][ref] = sub;
            if (node.connected) {
                node.client.on('message', sub.handler);
                var options = {};
                options.qos = qos;
                node.client.subscribe(topic, options);
            }
        };

        this.unsubscribe = function (topic, ref) {
            ref = ref || 0;
            var sub = node.subscriptions[topic];
            if (sub) {
                if (sub[ref]) {
                    node.client.removeListener('message', sub[ref].handler);
                    delete sub[ref];
                }
                if (Object.keys(sub).length === 0) {
                    delete node.subscriptions[topic];
                    if (node.connected) {
                        node.client.unsubscribe(topic);
                    }
                }
            }
        };

        this.publish = function (msg) {
            if (node.connected) {
                if (!Buffer.isBuffer(msg.payload)) {
                    if (typeof msg.payload === "object") {
                        msg.payload = JSON.stringify(msg.payload);
                    } else if (typeof msg.payload !== "string") {
                        msg.payload = "" + msg.payload;
                    }
                }

                var options = {
                    qos: msg.qos || 0,
                    retain: msg.retain || false
                };
                node.client.publish(msg.topic, msg.payload, options, function (err) {
                    if (err) {
                        node.error("error publishing message: " + err.toString());
                    }
                    return
                });
            }
        };

        function deleteStore(removed, done) {
            if (removed) {
                fs.remove(path.join(mqttDir,  n.id), function(err) {
                    if (err) {
                        node.warn("database delete failed: "+err.toString());
                    }
                    done();
                });
            } else {
                done();
            }
        }
        
        this.on('close', function (removed, done) {
            this.closing = true;
            if (this.connected) {
                this.client.once('close', function () {
                    deleteStore(removed, done);
                });
                this.client.end();
            } else if (this.connecting || node.client.reconnecting) {
                node.client.end();
                deleteStore(removed, done);
            } else {
                deleteStore(removed, done);
            }
        }); 
    }

    function MQTTInNode(n) {
        RED.nodes.createNode(this, n);
        this.qos = parseInt(n.qos);
        if (isNaN(this.qos) || this.qos < 0 || this.qos > 1) {
            this.qos = 1;
        }
        this.broker = n.broker;
        this.brokerConn = RED.nodes.getNode(this.broker);
        this.topic = '/devices/' + this.brokerConn.deviceid + '/config';
        this.topic2 = '/devices/' + this.brokerConn.deviceid + '/commands/#';
        if (!/^(#$|(\+|[^+#]*)(\/(\+|[^+#]*))*(\/(\+|#|[^+#]*))?$)/.test(this.topic)) {
            return this.warn(RED._("google-iot-core.errors.invalid-topic"));
        }
        if (!/^(#$|(\+|[^+#]*)(\/(\+|[^+#]*))*(\/(\+|#|[^+#]*))?$)/.test(this.topic2)) {
            return this.warn(RED._("google-iot-core.errors.invalid-topic"));
        }        
        var node = this;
        if (this.brokerConn) {
            this.status({ fill: "red", shape: "ring", text: "node-red:common.status.disconnected" });
            if (this.topic) {
                node.brokerConn.register(this);
                this.brokerConn.subscribe(this.topic, this.qos, function (topic, payload, packet) {
                    if (isUtf8(payload)) { payload = payload.toString(); }
                    var msg = { topic: topic, payload: payload, qos: packet.qos, retain: packet.retain };
                    if ((node.brokerConn.broker === "localhost") || (node.brokerConn.broker === "127.0.0.1")) {
                        msg._topic = topic;
                    }
                    node.send(msg);
                }, this.id);
                if (this.brokerConn.connected) {
                    node.status({ fill: "green", shape: "dot", text: "node-red:common.status.connected" });
                }
            }
            else {
                this.error(RED._("google-iot-core.errors.not-defined"));
            }
            if (this.topic2) {
                node.brokerConn.register(this);
                this.brokerConn.subscribe(this.topic2, this.qos, function (topic, payload, packet) {
                    if (isUtf8(payload)) { payload = payload.toString(); }
                    var msg = { topic: topic, payload: payload, qos: packet.qos, retain: packet.retain };
                    if ((node.brokerConn.broker === "localhost") || (node.brokerConn.broker === "127.0.0.1")) {
                        msg._topic = topic;
                    }
                    node.send(msg);
                }, this.id);
                if (this.brokerConn.connected) {
                    node.status({ fill: "green", shape: "dot", text: "node-red:common.status.connected" });
                }
            }
            else {
                this.error(RED._("google-iot-core.errors.not-defined"));
            }            
            this.on('close', function (done) {
                if (node.brokerConn) {
                    node.brokerConn.unsubscribe(node.topic, node.id);
                    node.brokerConn.deregister(node, done);
                }
            });
        } else {
            this.error(RED._("google-iot-core.errors.missing-config"));
        }
    }
    RED.nodes.registerType("google-iot-core in", MQTTInNode);

    RED.nodes.registerType("google-iot-core-broker", MQTTBrokerNode, {
    });

    function MQTTOutNode(n) {
        RED.nodes.createNode(this, n);
        this.qos = n.qos || null;
        this.retain = n.retain;
        this.broker = n.broker;
        this.subfolder = n.subfolder;
        this.brokerConn = RED.nodes.getNode(this.broker);

        if (this.subfolder) {
            this.topic = '/devices/' + this.brokerConn.deviceid + '/events/' + this.subfolder;
        } else {
            this.topic = '/devices/' + this.brokerConn.deviceid + '/events';
        }
        var node = this;

        if (this.brokerConn) {
            this.status({ fill: "red", shape: "ring", text: "node-red:common.status.disconnected" });
            this.on("input", function (msg) {
                if (msg.qos) {
                    msg.qos = parseInt(msg.qos);
                    if ((msg.qos !== 0) && (msg.qos !== 1)) {
                        msg.qos = null;
                    }
                }
                msg.qos = Number(node.qos || msg.qos || 0);
                msg.retain = node.retain || msg.retain || false;
                msg.retain = ((msg.retain === true) || (msg.retain === "true")) || false;
                if (node.topic) {
                    msg.topic = node.topic;
                }
                if (msg.hasOwnProperty("payload")) {
                    if (msg.hasOwnProperty("topic") && (typeof msg.topic === "string") && (msg.topic !== "")) { // topic must exist
                        this.brokerConn.publish(msg);  // send the message
                    }
                    else { node.warn(RED._("google-iot-core.errors.invalid-topic")); }
                }
            });
            if (this.brokerConn.connected) {
                node.status({ fill: "green", shape: "dot", text: "node-red:common.status.connected" });
            }
            node.brokerConn.register(node);
            this.on('close', function (done) {
                node.brokerConn.deregister(node, done);
            });
        } else {
            this.error(RED._("google-iot-core.errors.missing-config"));
        }
    }
    RED.nodes.registerType("google-iot-core out", MQTTOutNode);
};
