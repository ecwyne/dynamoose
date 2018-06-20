import Attribute, {
    create,
    createUnknownAttrbuteFromDynamo,
} from './Attribute';
import Model from './Model';
import { SchemaError } from './errors';
import VirtualType from './VirtualType';
import * as Debug from 'debug';

const debug = Debug('dynamoose:schema');

interface IThroughput {
    read: number;
    write: number;
}

interface ITimestamps {
    createdAt: string;
    updatedAt: string;
}

interface IExpires {
    ttl: number;
    attribute: string;
}

type Obj = { [key: string]: any };

export interface ISchemaOptions {
    throughput?: number | IThroughput;
    timestamps?: boolean | ITimestamps;
    saveUnknown?: boolean | string[];
    useDocumentTypes?: boolean;
    useNativeBooleans?: boolean;
    attributeFromDynamo?: (
        name: string,
        json: any,
        defaultFormatter: Function,
        model: Model,
    ) => any;
    attributeToDynamo?: (
        name: string,
        json: any,
        model: Model,
        defaultFormatter: Function,
        options: { [key: string]: any },
    ) => { [key: string]: any };
    expires?: number | IExpires;
}

export default class Schema {
    methods: { [key: string]: Function } = {};
    statics: { [key: string]: Function } = {};
    virtuals: { [key: string]: VirtualType } = {};
    tree: { [key: string]: any } = {};
    throughput: IThroughput;
    timestamps?: ITimestamps;
    expires?: IExpires;
    useDocumentTypes?: boolean;
    useNativeBooleans?: boolean;
    attributeFromDynamo?: (
        name: string,
        json: any,
        defaultFormatter: Function,
        model: Model,
    ) => any;
    attributeToDynamo?: (
        name: string,
        json: any,
        model: Model,
        defaultFormatter: Function,
        options: { [key: string]: any },
    ) => { [key: string]: any };
    attributes: { [key: string]: Attribute };
    indexes: any;
    hashKey!: Attribute;
    rangeKey!: Attribute;
    constructor(obj: Obj, public options: ISchemaOptions = {}) {
        debug('Creating Schema', obj);
        if (this.options.throughput) {
            var throughput = this.options.throughput;
            if (typeof throughput === 'number') {
                throughput = { read: throughput, write: throughput };
            }
            this.throughput = throughput;
        } else {
            this.throughput = { read: 1, write: 1 };
        }

        if (
            (!this.throughput.read || !this.throughput.write) &&
            this.throughput.read >= 1 &&
            this.throughput.write >= 1
        ) {
            throw new SchemaError('Invalid throughput: ' + this.throughput);
        }

        if (this.options.timestamps) {
            var createdAt = null;
            var updatedAt = null;

            if (this.options.timestamps === true) {
                createdAt = 'createdAt';
                updatedAt = 'updatedAt';
            } else if (typeof this.options.timestamps === 'object') {
                if (
                    this.options.timestamps.createdAt &&
                    this.options.timestamps.updatedAt
                ) {
                    createdAt = this.options.timestamps.createdAt;
                    updatedAt = this.options.timestamps.updatedAt;
                } else {
                    throw new SchemaError(
                        'Missing createdAt and updatedAt timestamps attribute. Maybe set timestamps: true?',
                    );
                }
            } else {
                throw new SchemaError('Invalid syntax for timestamp: ' + name);
            }

            obj[createdAt] = obj[createdAt] || {};
            obj[createdAt].type = Date;
            obj[createdAt].default = Date.now;

            obj[updatedAt] = obj[updatedAt] || {};
            obj[updatedAt].type = Date;
            obj[updatedAt].default = Date.now;
            obj[updatedAt].set = function() {
                return Date.now();
            };
            this.timestamps = { createdAt: createdAt, updatedAt: updatedAt };
        }

        if (
            this.options.expires !== null &&
            this.options.expires !== undefined
        ) {
            var expires: IExpires = { attribute: 'expires', ttl: 0 };

            if (typeof this.options.expires === 'number') {
                expires.ttl = this.options.expires;
            } else if (typeof this.options.expires === 'object') {
                if (typeof this.options.expires.ttl === 'number') {
                    expires.ttl = this.options.expires.ttl;
                } else {
                    throw new SchemaError(
                        'Missing or invalided ttl for expires attribute.',
                    );
                }
                if (typeof this.options.expires.attribute === 'string') {
                    expires.attribute = this.options.expires.attribute;
                }
            } else {
                throw new SchemaError('Invalid syntax for expires: ' + name);
            }

            var defaultExpires = function() {
                return new Date(Date.now() + expires.ttl * 1000);
            };

            obj[expires.attribute] = {
                type: Number,
                default: defaultExpires,
                set: function(v: Date) {
                    return Math.floor(v.getTime() / 1000);
                },
                get: function(v: number) {
                    return new Date(v * 1000);
                },
            };
            this.expires = expires;
        }
        this.useDocumentTypes = !!this.options.useDocumentTypes;
        this.useNativeBooleans = !!this.options.useNativeBooleans;
        this.attributeFromDynamo = this.options.attributeFromDynamo;
        this.attributeToDynamo = this.options.attributeToDynamo;

        this.attributes = {};
        this.indexes = { local: {}, global: {} };

        for (var n in obj) {
            if (this.attributes[n]) {
                throw new SchemaError('Duplicate attribute: ' + n);
            }

            debug('Adding Attribute to Schema (%s)', n, obj);
            this.attributes[n] = create(this, n, obj[n]);
        }
    }

    toDynamo(model: Model, options: { [key: string]: any } = {}) {
        var dynamoObj: { [key: string]: any } = {};
        var name: string, attr: Attribute;

        for (name in model) {
            if (!model.hasOwnProperty(name)) {
                continue;
            }
            if (
                model[name] === undefined ||
                model[name] === null ||
                Number.isNaN(model[name])
            ) {
                debug(
                    'toDynamo: skipping attribute: %s because its definition or value is null, undefined, or NaN',
                    name,
                );
                continue;
            }
            attr = this.attributes[name];
            if (
                (!attr && this.options.saveUnknown === true) ||
                (this.options.saveUnknown instanceof Array &&
                    this.options.saveUnknown.indexOf(name) >= 0)
            ) {
                attr = create(this, name, model[name]);
                this.attributes[name] = attr;
            }
        }

        for (name in this.attributes) {
            attr = this.attributes[name];

            attr.setDefault(model);
            var dynamoAttr;
            if (this.attributeToDynamo) {
                dynamoAttr = this.attributeToDynamo(
                    name,
                    model[name],
                    model,
                    attr.toDynamo.bind(attr),
                    options,
                );
            } else {
                dynamoAttr = attr.toDynamo(
                    model[name],
                    undefined,
                    model,
                    options,
                );
            }
            if (dynamoAttr) {
                dynamoObj[attr.name] = dynamoAttr;
            }
        }

        debug('toDynamo: %s', dynamoObj);
        return dynamoObj;
    }

    parseDynamo(model: Model, dynamoObj: { [key: string]: any }) {
        for (var name in dynamoObj) {
            var attr = this.attributes[name];

            if (
                (!attr && this.options.saveUnknown === true) ||
                (this.options.saveUnknown instanceof Array &&
                    this.options.saveUnknown.indexOf(name) >= 0)
            ) {
                attr = createUnknownAttrbuteFromDynamo(
                    this,
                    name,
                    dynamoObj[name],
                );
                this.attributes[name] = attr;
            }

            if (attr) {
                var attrVal;
                if (this.attributeFromDynamo) {
                    attrVal = this.attributeFromDynamo(
                        name,
                        dynamoObj[name],
                        attr.parseDynamo.bind(attr),
                        model,
                    );
                } else {
                    attrVal = attr.parseDynamo(dynamoObj[name]);
                }
                if (attrVal !== undefined && attrVal !== null) {
                    model[name] = attrVal;
                }
            } else {
                debug(
                    'parseDynamo: received an attribute name (%s) that is not defined in the schema',
                    name,
                );
            }
        }

        if (model.$__) {
            model.$__.originalItem = JSON.parse(JSON.stringify(model));
        }

        debug('parseDynamo: %s', model);

        return dynamoObj;
    }

    method(name: string | { [key: string]: Function }, fn: Function) {
        if (typeof name !== 'string') {
            for (var i in name) {
                this.methods[i] = name[i];
            }
        } else {
            this.methods[name] = fn;
        }
        return this;
    }

    static(name: string | { [key: string]: Function }, fn: Function) {
        if (typeof name !== 'string') {
            for (const i in name) {
                this.statics[i] = name[i];
            }
        } else {
            this.statics[name] = fn;
        }
        return this;
    }

    virtual(name: string, options: { [key: string]: any }) {
        const parts = name.split('.');

        return (this.virtuals[name] = parts.reduce(function(mem, part, i) {
            mem[part] ||
                (mem[part] =
                    i === parts.length - 1
                        ? new VirtualType(options, name)
                        : {});
            return mem[part];
        }, this.tree));
    }

    virtualpath(name: string) {
        return this.virtuals[name];
    }
}
