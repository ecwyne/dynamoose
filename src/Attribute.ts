import * as Debug from 'debug';
import * as util from 'util';
import Schema from './Schema';
import { SchemaError, ValidationError, ParseError } from './errors';
import Model from './Model';

const debug = Debug('dynamoose:attribute');

interface IAttributeOptions {
    required?: boolean;
    set?: Function;
    fromDynamo?: Function;
    toDynamo?: Function;
    get?: Function;
    default?: Function | any | null;
    validate?: null | RegExp | Function;
    index?: IIndex[] | IIndex | null;
    forceDefault?: boolean;
    trim?: boolean;
    lowercase?: boolean;
    uppercase?: boolean;
    enum?: any[];
    ref?: any;
}
interface IAttributeType {
    name: string;
    dynamo: string;
    dynamofy?: Function;
}

interface IIndex {
    global?: boolean;
    rangeKey?: any;
    throughput?: number | { read: number; write: number };
    name?: string;
    project?: boolean;
}

export default class Attribute {
    options: IAttributeOptions = {};
    type: IAttributeType;
    required?: boolean = false;
    attributes: { [key: string]: Attribute };
    set?: Function;
    parseDynamoCustom?: Function;
    toDynamoCustom?: Function;
    get?: Function;
    isSet: boolean = false;
    indexes: { [key: string]: { [key: string]: any } } = {};
    validator?: Function;
    default?: Function | any | null;
    constructor(public schema: Schema, public name: string, value: any) {
        debug('Creating attribute %s %o', name, value);
        if (value.type) {
            this.options = value;
        }
        this.type = this.schema.options.saveUnknown
            ? this.getTypeFromRawValue(value)
            : this.getType(value);

        if (!schema.useDocumentTypes) {
            if (this.type.name === 'map') {
                debug('Overwriting attribute %s type to object', name);
                this.type = this.types.object;
            } else if (this.type.name === 'list') {
                debug('Overwriting attribute %s type to array', name);
                this.type = this.types.array;
            }
        }

        if (schema.useNativeBooleans) {
            if (this.type.name === 'boolean') {
                debug(
                    'Overwriting attribute %s type to be a native boolean',
                    name,
                );
                this.type = this.types.nativeBoolean;
            }
        }

        this.attributes = {};

        if (this.type.name === 'map') {
            if (value.type) {
                value = value.map;
            }
            for (var subattrName in value) {
                if (this.attributes[subattrName]) {
                    throw new SchemaError(
                        'Duplicate attribute: ' +
                            subattrName +
                            ' in ' +
                            this.name,
                    );
                }

                this.attributes[subattrName] = create(
                    schema,
                    subattrName,
                    value[subattrName],
                );
            }
        } else if (this.type.name === 'list') {
            if (value.type) {
                value = value.list;
            }

            if (value === undefined && value[0] === undefined) {
                throw new SchemaError(
                    'No object given for attribute:' + this.name,
                );
            }
            // stang: Don't know what this guard is for - had to remove because when parsing unknown attributes, this is legal?
            // if (value.length > 1){
            //   throw new SchemaError('Only one object can be defined as a list type in ' + this.name );
            // }

            for (var i = 0; i < value.length; i++) {
                this.attributes[i] = create(schema, this.name, value[i]);
            }
        }

        if (this.options) {
            this.applyDefault(this.options.default);

            this.required = this.options.required;
            this.set = this.options.set;
            this.parseDynamoCustom = this.options.fromDynamo;
            this.toDynamoCustom = this.options.toDynamo;
            this.get = this.options.get;

            this.applyValidation(this.options.validate);

            this.applyIndexes(this.options.index);
        }
    }

    getTypeFromRawValue(value: any) {
        //no type defined - assume this is not a type definition and we must grab type directly from value
        let type;
        let typeVal = value;
        if (value.type) {
            typeVal = value.type;
        }

        if (util.isArray(typeVal) || typeVal === 'list') {
            type = 'List';
        } else if (
            (util.isArray(typeVal) && typeVal.length === 1) ||
            typeof typeVal === 'function'
        ) {
            this.isSet = util.isArray(typeVal);
            var regexFuncName = /^Function ([^(]+)\(/i;
            var found = typeVal.toString().match(regexFuncName);
            type = found[1];
            if (type === 'Object') {
                type = 'Map';
            }
        } else if (typeof typeVal === 'object' || typeVal === 'map') {
            type = 'Map';
        } else {
            type = typeof typeVal;
        }

        if (!type) {
            throw new SchemaError('Invalid attribute type: ' + type);
        }

        type = type.toLowerCase();

        if (!this.types[type]) {
            throw new SchemaError('Invalid attribute type: ' + type);
        }
        return this.types[type];
    }

    getType(value: any) {
        if (!value) {
            throw new SchemaError('Invalid attribute value: ' + value);
        }

        var type;
        var typeVal = value;
        if (value.type) {
            typeVal = value.type;
        }

        if (
            util.isArray(typeVal) &&
            typeVal.length === 1 &&
            typeof typeVal[0] === 'object'
        ) {
            type = 'List';
        } else if (
            (util.isArray(typeVal) && typeVal.length === 1) ||
            typeof typeVal === 'function'
        ) {
            this.isSet = util.isArray(typeVal);
            var regexFuncName = /^Function ([^(]+)\(/i;
            var found = typeVal.toString().match(regexFuncName);
            type = found[1];
        } else if (typeof typeVal === 'object') {
            type = 'Map';
        } else if (typeof typeVal === 'string') {
            type = typeVal;
        }

        if (!type) {
            throw new SchemaError('Invalid attribute type: ' + type);
        }

        type = type.toLowerCase();

        if (!this.types[type]) {
            throw new SchemaError('Invalid attribute type: ' + type);
        }
        return this.types[type];
    }

    applyDefault(dflt?: Function | any | null) {
        if (dflt === null || dflt === undefined) {
            delete this.default;
        } else if (typeof dflt === 'function') {
            this.default = dflt;
        } else {
            this.default = function() {
                return dflt;
            };
        }
    }

    applyValidation(validator?: null | RegExp | Function) {
        if (validator === null || validator === undefined) {
            delete this.validator;
        } else if (typeof validator === 'function') {
            this.validator = validator;
        } else if (validator.constructor.name === 'RegExp') {
            this.validator = function(val: string) {
                return validator.test(val);
            };
        } else {
            this.validator = function(val: any) {
                return validator === val;
            };
        }
    }

    applyIndexes(indexes?: IIndex[] | IIndex | null) {
        if (indexes === null || indexes === undefined) {
            delete this.indexes;
            return;
        }

        const attr = this;
        attr.indexes = {};

        function applyIndex(i: IIndex) {
            if (typeof i !== 'object') {
                i = {};
            }

            var index: IIndex = {};

            if (i.global) {
                index.global = true;

                if (i.rangeKey) {
                    index.rangeKey = i.rangeKey;
                }

                if (i.throughput) {
                    var throughput = i.throughput;
                    if (typeof throughput === 'number') {
                        throughput = { read: throughput, write: throughput };
                    }
                    index.throughput = throughput;
                    if (
                        (!index.throughput.read || !index.throughput.write) &&
                        index.throughput.read >= 1 &&
                        index.throughput.write >= 1
                    ) {
                        throw new SchemaError(
                            'Invalid Index throughput: ' + index.throughput,
                        );
                    }
                } else {
                    index.throughput = attr.schema.throughput;
                }
            }

            if (i.name) {
                index.name = i.name;
            } else {
                index.name =
                    attr.name + (i.global ? 'GlobalIndex' : 'LocalIndex');
            }

            if (i.project !== null && i.project !== undefined) {
                index.project = i.project;
            } else {
                index.project = true;
            }

            if (attr.indexes[index.name]) {
                throw new SchemaError('Duplicate index names: ' + index.name);
            }
            attr.indexes[index.name] = index;
        }

        if (util.isArray(indexes)) {
            indexes.map(applyIndex);
        } else {
            applyIndex(indexes);
        }
    }

    setDefault(model: Model) {
        if (model === undefined || model === null) {
            return;
        }
        var val = model[this.name];
        if (
            (val === null ||
                val === undefined ||
                val === '' ||
                this.options.forceDefault) &&
            this.default
        ) {
            model[this.name] = this.default(model);
            debug('Defaulted %s to %s', this.name, model[this.name]);
        }
    }

    toDynamo(val: any, noSet?: any, model?: any, options?: any) {
        if (this.toDynamoCustom) {
            return this.toDynamoCustom(val, noSet, model, options);
        }

        if (val === null || val === undefined || val === '') {
            if (this.required) {
                throw new ValidationError(
                    'Required value missing: ' + this.name,
                );
            }
            return null;
        }

        if (!noSet && this.isSet) {
            if (!util.isArray(val)) {
                throw new ValidationError('Values must be array: ' + this.name);
            }
            if (val.length === 0) {
                return null;
            }
        }

        if (this.validator && !this.validator(val, model)) {
            throw new ValidationError('Validation failed: ' + this.name);
        }

        // Check to see if attribute is a timestamp
        var isTimestamp = false;
        if (
            model &&
            model.$__ &&
            model.$__.schema &&
            model.$__.schema.timestamps &&
            (model.$__.schema.timestamps.createdAt === this.name ||
                model.$__.schema.timestamps.updatedAt === this.name)
        ) {
            isTimestamp = true;
        }

        var runSet = true;
        if (isTimestamp && options.updateTimestamps === false) {
            runSet = false;
        }
        if (this.set && runSet) {
            val = this.set(val);
        }

        var type = this.type;

        var isSet = this.isSet && !noSet;
        var dynamoObj: { [key: string]: any } = {};

        if (isSet) {
            dynamoObj[type.dynamo + 'S'] = val.map(
                function(this: Attribute, v: any) {
                    if (type.dynamofy) {
                        return type.dynamofy(v);
                    }
                    v = v.toString();
                    if (type.dynamo === 'S') {
                        if (this.options.trim) {
                            v = v.trim();
                        }
                        if (this.options.lowercase) {
                            v = v.toLowerCase();
                        }
                        if (this.options.uppercase) {
                            v = v.toUpperCase();
                        }
                    }

                    return v;
                }.bind(this),
            );
        } else if (type.name === 'map') {
            const dynamoMapObj: { [key: string]: any } = {};
            for (var name in this.attributes) {
                var attr = this.attributes[name];
                attr.setDefault(model);
                var dynamoAttr = attr.toDynamo(val[name], undefined, model);
                if (dynamoAttr) {
                    dynamoMapObj[attr.name] = dynamoAttr;
                }
            }
            dynamoObj.M = dynamoMapObj;
        } else if (type.name === 'list') {
            if (!util.isArray(val)) {
                throw new ValidationError(
                    'Values must be array in a `list`: ' + this.name,
                );
            }

            var dynamoList = [];

            for (var i = 0; i < val.length; i++) {
                var item = val[i];

                // TODO currently only supports one attribute type
                var objAttr = this.attributes[0];
                if (objAttr) {
                    objAttr.setDefault(model);
                    dynamoList.push(objAttr.toDynamo(item, undefined, model));
                }
            }
            dynamoObj.L = dynamoList;
        } else {
            if (type.dynamofy) {
                val = type.dynamofy(val);
            }

            if (type.dynamo !== 'BOOL') {
                val = val.toString();
            }

            if (type.dynamo === 'S') {
                if (this.options.enum) {
                    if (this.options.enum.indexOf(val) === -1) {
                        throw new ValidationError(
                            'Value must be one of : ' +
                                JSON.stringify(this.options.enum),
                        );
                    }
                }
                if (this.options.trim) {
                    val = val.trim();
                }
                if (this.options.lowercase) {
                    val = val.toLowerCase();
                }
                if (this.options.uppercase) {
                    val = val.toUpperCase();
                }
            }
            dynamoObj[type.dynamo] = val;
        }

        debug('toDynamo %j', dynamoObj);

        return dynamoObj;
    }

    parseDynamo(json: { [key: string]: any }) {
        if (this.parseDynamoCustom) {
            return this.parseDynamoCustom(json);
        }

        function dedynamofy(
            type: string,
            isSet: boolean,
            json: { [key: string]: any },
            transform: Function,
            attr: { [key: string]: any },
        ) {
            try {
                if (!json) {
                    return;
                }
                if (isSet) {
                    var set = json[type + 'S'];
                    return set.map(function(v: any) {
                        if (transform) {
                            return transform(v);
                        }
                        return v;
                    });
                }
                var val = json[type];
                if (transform) {
                    return transform(val !== undefined ? val : json, attr);
                }
                return val;
            } catch (e) {
                throw new ParseError(
                    'Attribute "' +
                        attr.name +
                        '" of type "' +
                        type +
                        '" has an invalid value of "' +
                        json[Object.keys(json)[0]] +
                        '"',
                    e,
                );
            }
        }

        function mapify(v: any, attr: Attribute) {
            if (!v) {
                return;
            }
            var val: { [key: string]: any } = {};

            for (var attrName in attr.attributes) {
                var attrVal = attr.attributes[attrName].parseDynamo(
                    v[attrName],
                );
                if (attrVal !== undefined && attrVal !== null) {
                    val[attrName] = attrVal;
                }
            }
            return val;
        }

        function listify(v: any, attr: Attribute) {
            if (!v) {
                return;
            }
            var val = [];
            debug('parsing list');

            if (util.isArray(v)) {
                for (var i = 0; i < v.length; i++) {
                    // TODO assume only one attribute type allowed for a list
                    var attrType = attr.attributes[0];
                    var attrVal = attrType.parseDynamo(v[i]);
                    if (attrVal !== undefined && attrVal !== null) {
                        val.push(attrVal);
                    }
                }
            }
            return val;
        }

        function datify(v: any) {
            debug('parsing date from %s', v);
            return new Date(parseInt(v, 10));
        }
        function bufferify(v: any) {
            return new Buffer(v);
        }
        function stringify(v: any) {
            if (typeof v !== 'string') {
                debug('******', v);
                return JSON.stringify(v);
            }
            return v;
        }

        var val;

        switch (this.type.name) {
            case 'string':
                val = dedynamofy('S', this.isSet, json, stringify, this);
                break;
            case 'number':
                val = dedynamofy('N', this.isSet, json, JSON.parse, this);
                break;
            case 'boolean':
                // 'S' is backwards compatible however 'BOOL' is a new valid argument
                val = dedynamofy(
                    this.type.dynamo,
                    this.isSet,
                    json,
                    JSON.parse,
                    this,
                );
                break;
            case 'date':
                val = dedynamofy('N', this.isSet, json, datify, this);
                break;
            case 'object':
                val = dedynamofy('S', this.isSet, json, JSON.parse, this);
                break;
            case 'array':
                val = dedynamofy('S', this.isSet, json, JSON.parse, this);
                break;
            case 'map':
                val = dedynamofy('M', this.isSet, json, mapify, this);
                break;
            case 'list':
                val = dedynamofy('L', this.isSet, json, listify, this);
                break;
            case 'buffer':
                val = dedynamofy('B', this.isSet, json, bufferify, this);
                break;
            default:
                throw new SchemaError('Invalid attribute type: ' + this.type);
        }

        if (this.get) {
            val = this.get(val);
        }

        debug('parseDynamo: %s : "%s" : %j', this.name, this.type.name, val);

        return val;
    }

    types: { [key: string]: IAttributeType } = {
        string: { name: 'string', dynamo: 'S' },
        number: { name: 'number', dynamo: 'N' },
        boolean: { name: 'boolean', dynamo: 'S', dynamofy: JSON.stringify },
        nativeBoolean: { name: 'boolean', dynamo: 'BOOL' },
        date: { name: 'date', dynamo: 'N', dynamofy: datify },
        object: { name: 'object', dynamo: 'S', dynamofy: JSON.stringify },
        array: { name: 'array', dynamo: 'S', dynamofy: JSON.stringify },
        map: { name: 'map', dynamo: 'M', dynamofy: JSON.stringify },
        list: { name: 'list', dynamo: 'L', dynamofy: JSON.stringify },
        buffer: { name: 'buffer', dynamo: 'B' },
    };
}

function datify(v: any) {
    if (!v.getTime) {
        v = new Date(v);
    }
    return JSON.stringify(v.getTime());
}

/*
 * Converts DynamoDB document types (Map and List) to dynamoose
 * attribute definition map and ist types
 *
 * For example, DynamoDB value:
 * {
 *   M: {
 *     subAttr1: { S: '' },
 *     subAttr2: { N: '' },
 *   }
 * }
 *
 * to
 * {
 *   type: 'map',
 *   map: {
 *     subAttr1: { type: String },
 *     subAttr1: { type: Number },
 *   },
 * }
 */
function createAttrDefFromDynamo(dynamoAttribute: { [key: string]: any }) {
    var dynamoType;
    var attrDef: { [key: string]: any } = {
        type: lookupType(dynamoAttribute),
    };
    if (attrDef.type === Object) {
        attrDef.type = 'map';
        for (dynamoType in dynamoAttribute) {
            attrDef.map = {};
            for (var subAttrName in dynamoAttribute[dynamoType]) {
                attrDef.map[subAttrName] = createAttrDefFromDynamo(
                    dynamoAttribute[dynamoType][subAttrName],
                );
            }
        }
    } else if (attrDef.type === Array) {
        attrDef.type = 'list';
        for (dynamoType in dynamoAttribute) {
            attrDef.list = dynamoAttribute[dynamoType].map(
                createAttrDefFromDynamo,
            );
        }
    }
    return attrDef;
}

export const createUnknownAttrbuteFromDynamo = function(
    schema: Schema,
    name: string,
    dynamoAttribute: any,
) {
    var attrDef = createAttrDefFromDynamo(dynamoAttribute);
    var attr = new Attribute(schema, name, attrDef);
    return attr;
};

export const create = function(
    schema: Schema,
    name: string,
    obj: { [key: string]: any },
) {
    const value = obj;
    const options = typeof obj === 'object' && obj.type ? obj : {};
    const attr = new Attribute(schema, name, value);

    if (options.hashKey && options.rangeKey) {
        throw new SchemaError('Cannot be both hashKey and rangeKey: ' + name);
    }

    if (options.hashKey || (!schema.hashKey && !options.rangeKey)) {
        schema.hashKey = attr;
    }

    if (options.rangeKey) {
        schema.rangeKey = attr;
    }

    // check for global attributes in the tree..
    if (attr.indexes) {
        for (var indexName in attr.indexes) {
            var index = attr.indexes[indexName];
            if (
                schema.indexes.global[indexName] ||
                schema.indexes.local[indexName]
            ) {
                throw new SchemaError('Duplicate index name: ' + indexName);
            }
            if (index.global) {
                schema.indexes.global[indexName] = attr;
            } else {
                schema.indexes.local[indexName] = attr;
            }
        }
    }

    return attr;
};

export const lookupType = function(dynamoObj: any) {
    if (dynamoObj.S !== null && dynamoObj.S !== undefined) {
        try {
            JSON.parse(dynamoObj.S);
            return Object;
        } catch (err) {
            return String;
        }
        return [Number];
    }
    if (dynamoObj.L !== null && dynamoObj.L !== undefined) {
        return Array;
    }
    if (dynamoObj.M !== null && dynamoObj.M !== undefined) {
        return Object;
    }
    if (dynamoObj.N !== null && dynamoObj.N !== undefined) {
        return Number;
    }
    if (dynamoObj.BOOL !== null && dynamoObj.BOOL !== undefined) {
        return Boolean;
    }
    if (dynamoObj.B !== null && dynamoObj.B !== undefined) {
        return Buffer;
    }
    if (dynamoObj.NS !== null && dynamoObj.NS !== undefined) {
        return [Number];
    }
};
