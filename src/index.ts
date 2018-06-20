import Schema, { ISchemaOptions } from './Schema';
import Model from './Model';
import Table from './Table';
import VirtualType from './VirtualType';
import * as https from 'https';
import * as AWS from 'aws-sdk';
import * as Debug from 'debug';

const debug = Debug('dynamoose');

const createLocalDb = (endpoint: string) => new AWS.DynamoDB({ endpoint });

interface IDynamooseDefaults {
    create?: boolean;
    waitForActive?: boolean;
    waitForActiveTimeout?: number;
    prefix?: string;
    suffix?: string;
}

export class Dynamoose {
    endpointURL?: string;
    dynamoDB?: AWS.DynamoDB;
    models: { [key: string]: Model };
    defaults: IDynamooseDefaults;
    dynamoDocumentClient?: AWS.DynamoDB.DocumentClient;
    constructor() {
        this.models = {};

        this.defaults = {
            create: true,
            waitForActive: true,
            waitForActiveTimeout: 180000,
            prefix: '',
            suffix: '',
        };
    }

    model(
        name: string,
        schema: Schema,
        options: IDynamooseDefaults & ISchemaOptions = {},
    ) {
        options = {
            ...this.defaults,
            ...options,
        };

        name = options.prefix + name + options.suffix;

        debug('Looking up model %s', name);

        if (this.models[name]) {
            return this.models[name];
        }
        if (!(schema instanceof Schema)) {
            schema = new Schema(schema, options);
        }

        const model = Model.compile(name, schema, options, this);
        this.models[name] = model;
        return model;
    }

    VirtualType = VirtualType;

    AWS = AWS;

    local(url: string = 'http://localhost:8000') {
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
        } else {
            debug('Getting default dynamodb document client');
        }
        this.dynamoDocumentClient = new this.AWS.DynamoDB.DocumentClient();
        return this.dynamoDocumentClient;
    }

    setDocumentClient(documentClient: AWS.DynamoDB.DocumentClient) {
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
        } else {
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

    setDefaults(options: IDynamooseDefaults) {
        Object.assign(this.defaults, options);
    }

    Schema = Schema;
    Table = Table;
    Dynamoose = Dynamoose;
}

export default new Dynamoose();
