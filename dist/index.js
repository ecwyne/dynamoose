"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const Schema_1 = require("./Schema");
const Model_1 = require("./Model");
const Table_1 = require("./Table");
const VirtualType_1 = require("./VirtualType");
const https = require("https");
const AWS = require("aws-sdk");
const Debug = require("debug");
const debug = Debug('dynamoose');
const createLocalDb = (endpoint) => new AWS.DynamoDB({ endpoint });
class Dynamoose {
    constructor() {
        this.VirtualType = VirtualType_1.default;
        this.AWS = AWS;
        this.Schema = Schema_1.default;
        this.Table = Table_1.default;
        this.Dynamoose = Dynamoose;
        this.models = {};
        this.defaults = {
            create: true,
            waitForActive: true,
            waitForActiveTimeout: 180000,
            prefix: '',
            suffix: '',
        };
    }
    model(name, schema, options = {}) {
        options = Object.assign({}, this.defaults, options);
        name = options.prefix + name + options.suffix;
        debug('Looking up model %s', name);
        if (this.models[name]) {
            return this.models[name];
        }
        if (!(schema instanceof Schema_1.default)) {
            schema = new Schema_1.default(schema, options);
        }
        const model = Model_1.default.compile(name, schema, options, this);
        this.models[name] = model;
        return model;
    }
    local(url = 'http://localhost:8000') {
        this.endpointURL = url;
        this.dynamoDB = createLocalDb(this.endpointURL);
        debug('Setting DynamoDB to local (%s)', this.endpointURL);
    }
    documentClient() {
        if (this.dynamoDocumentClient) {
            return this.dynamoDocumentClient;
        }
        if (this.endpointURL) {
            debug('Setting dynamodb document client to %s', this.endpointURL);
            // this.AWS.config.update({
            //     endpoint: this.endpointURL,
            // });
        }
        else {
            debug('Getting default dynamodb document client');
        }
        this.dynamoDocumentClient = new this.AWS.DynamoDB.DocumentClient();
        return this.dynamoDocumentClient;
    }
    setDocumentClient(documentClient) {
        debug('Setting dynamodb document client');
        this.dynamoDocumentClient = documentClient;
    }
    ddb() {
        if (this.dynamoDB) {
            return this.dynamoDB;
        }
        if (this.endpointURL) {
            debug('Setting DynamoDB to %s', this.endpointURL);
            this.dynamoDB = createLocalDb(this.endpointURL);
        }
        else {
            debug('Getting default DynamoDB');
            this.dynamoDB = new this.AWS.DynamoDB({
                httpOptions: {
                    agent: new https.Agent({
                        rejectUnauthorized: true,
                        keepAlive: true,
                    }),
                },
            });
        }
        return this.dynamoDB;
    }
    setDefaults(options) {
        Object.assign(this.defaults, options);
    }
}
exports.Dynamoose = Dynamoose;
exports.default = new Dynamoose();
