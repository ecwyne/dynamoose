"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const Q = require("q");
const hooks_1 = require("hooks");
const Table_1 = require("./Table");
const Query_1 = require("./Query");
const Scan_1 = require("./Scan");
const errors_1 = require("./errors");
const reserved_keywords_1 = require("./reserved-keywords");
const lodash_1 = require("lodash");
const Debug = require("debug");
const debug = Debug('dynamoose:model');
//var MAX_BATCH_READ_SIZE   = 100;
const MAX_BATCH_WRITE_SIZE = 25;
class Model {
    constructor(obj = {}) {
        this.save = this.put;
        Object.assign(this, obj);
    }
    put(options, next) {
        debug('put', this);
        var deferred = Q.defer();
        function putItem() {
            model$.base.ddb().putItem(item, function (err) {
                if (err) {
                    deferred.reject(err);
                }
                deferred.resolve(model);
            });
        }
        try {
            options = options || {};
            if (typeof options === 'function') {
                next = options;
                options = {};
            }
            if (options.overwrite === null || options.overwrite === undefined) {
                options.overwrite = true;
            }
            var toDynamoOptions = { updateTimestamps: true };
            if (options.updateTimestamps === false) {
                toDynamoOptions.updateTimestamps = false;
            }
            var schema = (this.$__ && this.$__.schema);
            var item = {
                TableName: this.$__ && this.$__.name,
                Item: schema.toDynamo(this, toDynamoOptions),
            };
            schema.parseDynamo(this, item.Item);
            if (!options.overwrite) {
                if (!reserved_keywords_1.isReservedKeyword(schema.hashKey.name)) {
                    item.ConditionExpression =
                        'attribute_not_exists(' +
                            (schema.hashKey && schema.hashKey.name) +
                            ')';
                }
                else {
                    item.ConditionExpression =
                        'attribute_not_exists(#__hash_key)';
                    item.ExpressionAttributeNames =
                        item.ExpressionAttributeNames || {};
                    item.ExpressionAttributeNames['#__hash_key'] =
                        schema.hashKey && schema.hashKey.name;
                }
            }
            processCondition(item, options, schema);
            debug('putItem', item);
            var model = this;
            var model$ = this.$__;
            if (model$.options.waitForActive) {
                model$.table
                    .waitForActive()
                    .then(putItem)
                    .catch(deferred.reject);
            }
            else {
                putItem();
            }
        }
        catch (err) {
            deferred.reject(err);
        }
        return deferred.promise.nodeify(next);
    }
    static populate(options, resultObj, fillPath) {
        if (!fillPath) {
            fillPath = [];
        }
        var self = this;
        if (!resultObj) {
            resultObj = self;
        }
        var ModelPathName = '';
        if (typeof options === 'string') {
            ModelPathName = this.prototype.$__.table.schema.attributes[options]
                .options.ref;
        }
        else if (options.path) {
            ModelPathName = this.prototype.$__.table.schema.attributes[options.path].options.ref;
        }
        else if (!options.path && !options.model) {
            throw new Error('Invalid parameters provided to the populate method');
        }
        var Model = lodash_1.filter(this.prototype.$__.table.base.models, function (model) {
            return (model.$__.name ===
                model.$__.options.prefix +
                    options.model +
                    model.$__.options.suffix ||
                model.$__.name === model.$__.options.prefix + options.model ||
                model.$__.name === options.model + model.$__.options.suffix ||
                model.$__.name === options.model ||
                model.$__.name ===
                    model.$__.options.prefix +
                        ModelPathName +
                        model.$__.options.suffix ||
                model.$__.name === model.$__.options.prefix + ModelPathName ||
                model.$__.name === ModelPathName + model.$__.options.suffix ||
                model.$__.name === ModelPathName);
        }).pop();
        if (!Model) {
            throw new Error("The provided model doesn't exists");
        }
        return Model.get(self[options.path || options]).then(function (target) {
            if (!target) {
                throw new Error('Invalid reference');
            }
            self[options.path || options] = target;
            fillPath = fillPath.concat(options.path || options);
            lodash_1.set(resultObj, fillPath, target);
            if (options.populate) {
                return self[options.path || options].populate(options.populate, resultObj, fillPath);
            }
            else {
                return resultObj;
            }
        });
    }
    delete(options, next) {
        options = options || {};
        if (typeof options === 'function') {
            next = options;
            options = {};
        }
        var schema = this.$__.schema;
        var hashKeyName = (schema.hashKey && schema.hashKey.name);
        var deferred = Q.defer();
        if (this[hashKeyName] === null || this[hashKeyName] === undefined) {
            deferred.reject(new errors_1.ModelError('Hash key required: ' + hashKeyName));
            return deferred.promise.nodeify(next);
        }
        if (schema.rangeKey &&
            (this[schema.rangeKey.name] === null ||
                this[schema.rangeKey.name] === undefined)) {
            deferred.reject(new errors_1.ModelError('Range key required: ' +
                (schema.hashKey && schema.hashKey.name)));
            return deferred.promise.nodeify(next);
        }
        var getDelete = { TableName: this.$__.name, Key: {} };
        try {
            getDelete.Key[hashKeyName] =
                schema.hashKey &&
                    schema.hashKey.toDynamo(this[hashKeyName], undefined, this);
            if (schema.rangeKey) {
                var rangeKeyName = schema.rangeKey.name;
                getDelete.Key[rangeKeyName] = schema.rangeKey.toDynamo(this[rangeKeyName], undefined, this);
            }
        }
        catch (err) {
            deferred.reject(err);
            return deferred.promise.nodeify(next);
        }
        if (options.update) {
            getDelete.ReturnValues = 'ALL_OLD';
            getDelete.ConditionExpression =
                'attribute_exists(' +
                    (schema.hashKey && schema.hashKey.name) +
                    ')';
        }
        var model = this;
        var model$ = this.$__;
        function deleteItem() {
            debug('deleteItem', getDelete);
            model$.base.ddb().deleteItem(getDelete, function (err, data) {
                if (err) {
                    debug('Error returned by deleteItem', err);
                    return deferred.reject(err);
                }
                debug('deleteItem response', data);
                if (options.update && data.Attributes) {
                    try {
                        schema.parseDynamo(model, data.Attributes);
                        debug('deleteItem parsed model', model);
                    }
                    catch (err) {
                        return deferred.reject(err);
                    }
                }
                deferred.resolve(model);
            });
        }
        if (model$.options.waitForActive) {
            model$.table
                .waitForActive()
                .then(deleteItem)
                .catch(deferred.reject);
        }
        else {
            deleteItem();
        }
        return deferred.promise.nodeify(next);
    }
    static compile(name, schema, options, base) {
        debug('compiling NewModel %s', name);
        const table = new Table_1.default(name, schema, options, base);
        /*jshint validthis: true */
        class NewModel {
            constructor(obj) {
                Object.assign(this, obj);
                applyVirtuals(this, schema);
                Object.assign(this.$__, {
                    table,
                    base,
                    name,
                    schema,
                    options,
                    originalItem: obj,
                });
            }
            originalItem() {
                return this.$__.originalItem;
            }
            static get(key, options, next) {
                try {
                    return Model.get(NewModel, key, options, next);
                }
                catch (err) {
                    sendErrorToCallback(err, options, next);
                    return Q.reject(err);
                }
            }
            static populate(options, resultObj, fillPath) {
                try {
                    return Model.populate(options, resultObj, fillPath);
                }
                catch (err) {
                    sendErrorToCallback(err, options);
                    return Q.reject(err);
                }
            }
            static update(key, update, options, next) {
                try {
                    return Model.update(NewModel, key, update, options, next);
                }
                catch (err) {
                    sendErrorToCallback(err, options, next);
                    return Q.reject(err);
                }
            }
            static delete(key, options, next) {
                try {
                    return Model.delete(NewModel, key, options, next);
                }
                catch (err) {
                    sendErrorToCallback(err, options, next);
                    return Q.reject(err);
                }
            }
            static query(query, options, next) {
                try {
                    return Model.query(NewModel, query, options, next);
                }
                catch (err) {
                    sendErrorToCallback(err, options, next);
                    return Q.reject(err);
                }
            }
            static queryOne(query, options, next) {
                try {
                    return Model.queryOne(NewModel, query, options, next);
                }
                catch (err) {
                    sendErrorToCallback(err, options, next);
                    return Q.reject(err);
                }
            }
            static scan(filter, options, next) {
                try {
                    return Model.scan(NewModel, filter, options, next);
                }
                catch (err) {
                    sendErrorToCallback(err, options, next);
                    return Q.reject(err);
                }
            }
            static create(obj, options, next) {
                try {
                    return Model.create(NewModel, obj, options, next);
                }
                catch (err) {
                    sendErrorToCallback(err, options, next);
                    return Q.reject(err);
                }
            }
            static batchGet(keys, options, next) {
                try {
                    return Model.batchGet(NewModel, keys, options, next);
                }
                catch (err) {
                    sendErrorToCallback(err, options, next);
                    return Q.reject(err);
                }
            }
            static batchPut(keys, options, next) {
                try {
                    return Model.batchPut(NewModel, keys, options, next);
                }
                catch (err) {
                    sendErrorToCallback(err, options, next);
                    return Q.reject(err);
                }
            }
            static batchDelete(keys, options, next) {
                try {
                    return Model.batchDelete(NewModel, keys, options, next);
                }
                catch (err) {
                    sendErrorToCallback(err, options, next);
                    return Q.reject(err);
                }
            }
            static waitForActive(timeout, next) {
                return table.waitForActive(timeout, next);
            }
            static getTableReq() {
                return table.buildTableReq(table.name, table.schema);
            }
        }
        NewModel.$__ = {};
        // Set NewModel.name to match name of table. Original property descriptor
        // values are reused.
        const nameDescriptor = Object.getOwnPropertyDescriptor(NewModel, 'name');
        // Skip if 'name' property can not be redefined. This probably means
        // code is running in a "non-standard, pre-ES2015 implementations",
        // like node 0.12.
        if (nameDescriptor && nameDescriptor.configurable) {
            nameDescriptor.value = 'Model-' + name;
            Object.defineProperty(NewModel, 'name', nameDescriptor);
        }
        NewModel.$__ = NewModel.prototype.$__;
        // apply methods and statics
        applyMethods(NewModel, schema);
        applyStatics(NewModel, schema);
        // set up middleware
        Object.assign(NewModel, hooks_1.default);
        table.init(function (err) {
            if (err) {
                throw err;
            }
        });
        return NewModel;
    }
    static create(NewModel, obj, options, next) {
        options = options || {};
        if (typeof options === 'function') {
            next = options;
            options = {};
        }
        if (options.overwrite === null || options.overwrite === undefined) {
            options.overwrite = false;
        }
        var model = new NewModel(obj);
        return model.save(options, next);
    }
    static get(NewModel, key, options, next) {
        debug('Get %j', key);
        var deferred = Q.defer();
        options = options || {};
        if (typeof options === 'function') {
            next = options;
            options = {};
        }
        if (key === null || key === undefined) {
            deferred.reject(new errors_1.ModelError('Key required to get item'));
            return deferred.promise.nodeify(next);
        }
        var schema = NewModel.$__.schema;
        function validKeyValue(value) {
            return value !== undefined && value !== null && value !== '';
        }
        var hashKeyName = schema.hashKey && schema.hashKey.name;
        if (!validKeyValue(key[hashKeyName])) {
            var keyVal = key;
            key = {};
            key[hashKeyName] = keyVal;
        }
        if (schema.rangeKey && !validKeyValue(key[schema.rangeKey.name])) {
            deferred.reject(new errors_1.ModelError('Range key required: ' + schema.rangeKey.name));
            return deferred.promise.nodeify(next);
        }
        var getReq = { TableName: NewModel.$__.name, Key: {} };
        getReq.Key[hashKeyName] =
            schema.hashKey &&
                schema.hashKey.toDynamo(key[hashKeyName], undefined, key);
        if (schema.rangeKey) {
            var rangeKeyName = schema.rangeKey.name;
            getReq.Key[rangeKeyName] = schema.rangeKey.toDynamo(key[rangeKeyName], undefined, key);
        }
        if (options.attributes) {
            getReq.AttributesToGet = options.attributes;
        }
        if (options.consistent) {
            getReq.ConsistentRead = true;
        }
        var newModel$ = NewModel.$__;
        function get() {
            debug('getItem', getReq);
            newModel$.base
                .ddb()
                .getItem(getReq, function (err, data) {
                if (err) {
                    debug('Error returned by getItem', err);
                    return deferred.reject(err);
                }
                debug('getItem response', data);
                if (!Object.keys(data).length) {
                    return deferred.resolve();
                }
                var model = new NewModel();
                model.$__.isNew = false;
                try {
                    schema.parseDynamo(model, data.Item);
                }
                catch (e) {
                    debug('cannot parse data', data);
                    return deferred.reject(e);
                }
                debug('getItem parsed model', model);
                deferred.resolve(model);
            });
        }
        if (newModel$.options.waitForActive) {
            newModel$.table
                .waitForActive()
                .then(get)
                .catch(deferred.reject);
        }
        else {
            get();
        }
        return deferred.promise.nodeify(next);
    }
    static update(NewModel, key, update, options, next) {
        debug('Update %j', key);
        var deferred = Q.defer();
        var schema = NewModel.$__.schema;
        if (typeof update === 'function') {
            next = update;
            update = null;
        }
        if (update === undefined || update === null) {
            update = key;
        }
        options = options || {};
        if (typeof options === 'function') {
            next = options;
            options = {};
        }
        // default createRequired to false
        if (typeof options.createRequired === 'undefined') {
            options.createRequired = false;
        }
        // default updateTimestamps to true
        if (typeof options.updateTimestamps === 'undefined') {
            options.updateTimestamps = true;
        }
        // default return values to 'NEW_ALL'
        if (typeof options.returnValues === 'undefined') {
            options.returnValues = 'ALL_NEW';
        }
        // if the key part was emtpy, try the key defaults before giving up...
        if (key === null || key === undefined) {
            key = {};
            // first figure out the primary/hash key
            var hashKeyDefault = schema.attributes[schema.hashKey.name].options.default;
            if (typeof hashKeyDefault === 'undefined') {
                deferred.reject(new errors_1.ModelError('Key required to get item'));
                return deferred.promise.nodeify(next);
            }
            key[schema.hashKey.name] =
                typeof hashKeyDefault === 'function'
                    ? hashKeyDefault()
                    : hashKeyDefault;
            // now see if you have to figure out a range key
            if (schema.rangeKey) {
                var rangeKeyDefault = schema.attributes[schema.rangeKey.name].options.default;
                if (typeof rangeKeyDefault === 'undefined') {
                    deferred.reject(new errors_1.ModelError('Range key required: ' + schema.rangeKey.name));
                    return deferred.promise.nodeify(next);
                }
                key[schema.rangeKey.name] =
                    typeof rangeKeyDefault === 'function'
                        ? rangeKeyDefault()
                        : rangeKeyDefault;
            }
        }
        var hashKeyName = schema.hashKey && schema.hashKey.name;
        if (!key[hashKeyName]) {
            var keyVal = key;
            key = {};
            key[hashKeyName] = keyVal;
        }
        var updateReq = {
            TableName: NewModel.$__.name,
            Key: {},
            ExpressionAttributeNames: {},
            ExpressionAttributeValues: {},
            ReturnValues: options.returnValues,
        };
        processCondition(updateReq, options, NewModel.$__.schema);
        updateReq.Key[hashKeyName] =
            schema.hashKey &&
                schema.hashKey.toDynamo(key[hashKeyName], undefined, key);
        if (schema.rangeKey) {
            var rangeKeyName = schema.rangeKey.name;
            (updateReq.Key[rangeKeyName] = schema.rangeKey.toDynamo(key[rangeKeyName])),
                undefined,
                key;
        }
        // determine the set of operations to be executed
        class Operations {
            constructor() {
                this.ifNotExistsSet = {};
                this.SET = {};
                this.ADD = {};
                this.REMOVE = {};
            }
            addIfNotExistsSet(name, item) {
                this.ifNotExistsSet[name] = item;
            }
            addSet(name, item) {
                if (schema.hashKey.name !== name &&
                    (schema.rangeKey || {}).name !== name) {
                    this.SET[name] = item;
                }
            }
            addAdd(name, item) {
                if (schema.hashKey.name !== name &&
                    (schema.rangeKey || {}).name !== name) {
                    this.ADD[name] = item;
                }
            }
            addRemove(name, item) {
                if (schema.hashKey.name !== name &&
                    (schema.rangeKey || {}).name !== name) {
                    this.REMOVE[name] = item;
                }
            }
            getUpdateExpression(updateReq) {
                var attrCount = 0;
                var updateExpression = '';
                var attrName;
                var valName;
                var name;
                var item;
                var setExpressions = [];
                for (name in this.ifNotExistsSet) {
                    item = this.ifNotExistsSet[name];
                    attrName = '#_n' + attrCount;
                    valName = ':_p' + attrCount;
                    updateReq.ExpressionAttributeNames[attrName] = name;
                    updateReq.ExpressionAttributeValues[valName] = item;
                    setExpressions.push(attrName +
                        ' = if_not_exists(' +
                        attrName +
                        ', ' +
                        valName +
                        ')');
                    attrCount += 1;
                }
                for (name in this.SET) {
                    item = this.SET[name];
                    attrName = '#_n' + attrCount;
                    valName = ':_p' + attrCount;
                    updateReq.ExpressionAttributeNames[attrName] = name;
                    updateReq.ExpressionAttributeValues[valName] = item;
                    setExpressions.push(attrName + ' = ' + valName);
                    attrCount += 1;
                }
                if (setExpressions.length > 0) {
                    updateExpression += 'SET ' + setExpressions.join(',') + ' ';
                }
                var addExpressions = [];
                for (name in this.ADD) {
                    item = this.ADD[name];
                    attrName = '#_n' + attrCount;
                    valName = ':_p' + attrCount;
                    updateReq.ExpressionAttributeNames[attrName] = name;
                    updateReq.ExpressionAttributeValues[valName] = item;
                    addExpressions.push(attrName + ' ' + valName);
                    attrCount += 1;
                }
                if (addExpressions.length > 0) {
                    updateExpression += 'ADD ' + addExpressions.join(',') + ' ';
                }
                var removeExpressions = [];
                for (name in this.REMOVE) {
                    item = this.REMOVE[name];
                    attrName = '#_n' + attrCount;
                    updateReq.ExpressionAttributeNames[attrName] = name;
                    removeExpressions.push(attrName);
                    attrCount += 1;
                }
                if (removeExpressions.length > 0) {
                    updateExpression += 'REMOVE ' + removeExpressions.join(',');
                }
                updateReq.UpdateExpression = updateExpression;
            }
        }
        var operations = new Operations();
        if (update.$PUT || (!update.$PUT && !update.$DELETE && !update.$ADD)) {
            var updatePUT = update.$PUT || update;
            for (var putItem in updatePUT) {
                var putAttr = schema.attributes[putItem];
                if (putAttr) {
                    var val = updatePUT[putItem];
                    var removeParams = val === null || val === undefined || val === '';
                    if (!options.allowEmptyArray) {
                        removeParams =
                            removeParams ||
                                (Array.isArray(val) && val.length === 0);
                    }
                    if (removeParams) {
                        operations.addRemove(putItem, null);
                    }
                    else {
                        try {
                            operations.addSet(putItem, putAttr.toDynamo(val));
                        }
                        catch (err) {
                            deferred.reject(err);
                            return deferred.promise.nodeify(next);
                        }
                    }
                }
            }
        }
        if (update.$DELETE) {
            for (var deleteItem in update.$DELETE) {
                var deleteAttr = schema.attributes[deleteItem];
                if (deleteAttr) {
                    var delVal = update.$DELETE[deleteItem];
                    if (delVal !== null && delVal !== undefined) {
                        try {
                            operations.addRemove(deleteItem, deleteAttr.toDynamo(delVal));
                        }
                        catch (err) {
                            deferred.reject(err);
                            return deferred.promise.nodeify(next);
                        }
                    }
                    else {
                        operations.addRemove(deleteItem, null);
                    }
                }
            }
        }
        if (update.$ADD) {
            for (var addItem in update.$ADD) {
                var addAttr = schema.attributes[addItem];
                if (addAttr) {
                    try {
                        operations.addAdd(addItem, addAttr.toDynamo(update.$ADD[addItem]));
                    }
                    catch (err) {
                        deferred.reject(err);
                        return deferred.promise.nodeify(next);
                    }
                }
            }
        }
        // update schema timestamps
        if (options.updateTimestamps && schema.timestamps) {
            var createdAtLabel = schema.timestamps.createdAt;
            var updatedAtLabel = schema.timestamps.updatedAt;
            var createdAtAttribute = schema.attributes[createdAtLabel];
            var updatedAtAttribute = schema.attributes[updatedAtLabel];
            var createdAtDefaultValue = createdAtAttribute.options.default();
            var updatedAtDefaultValue = updatedAtAttribute.options.default();
            operations.addIfNotExistsSet(createdAtLabel, createdAtAttribute.toDynamo(createdAtDefaultValue));
            operations.addSet(updatedAtLabel, updatedAtAttribute.toDynamo(updatedAtDefaultValue));
        }
        // do the required items check. Throw an error if you have an item that is required and
        //  doesn't have a default.
        if (options.createRequired) {
            for (var attributeName in schema.attributes) {
                var attribute = schema.attributes[attributeName];
                if (attribute.required &&
                    attributeName !== schema.hashKey &&
                    schema.hashKey.name &&
                    (!schema.rangeKey ||
                        attributeName !== schema.rangeKey.name) &&
                    (!schema.timestamps ||
                        attributeName !== schema.timestamps.createdAt) &&
                    (!schema.timestamps ||
                        attributeName !== schema.timestamps.updatedAt) &&
                    !operations.SET[attributeName] &&
                    !operations.ADD[attributeName] &&
                    !operations.REMOVE[attributeName]) {
                    // if the attribute is required... // ...and it isn't the hash key... // ...and it isn't the range key... // ...and it isn't the createdAt attribute... // ...and it isn't the updatedAt attribute...
                    var defaultValueOrFunction = attribute.options.default;
                    // throw an error if you have required attribute without a default (and you didn't supply
                    //  anything to update with)
                    if (typeof defaultValueOrFunction === 'undefined') {
                        var err = 'Required attribute "' +
                            attributeName +
                            '" does not have a default.';
                        debug('Error returned by updateItem', err);
                        deferred.reject(err);
                        return deferred.promise.nodeify(next);
                    }
                    var defaultValue = typeof defaultValueOrFunction === 'function'
                        ? defaultValueOrFunction()
                        : defaultValueOrFunction;
                    operations.addIfNotExistsSet(attributeName, attribute.toDynamo(defaultValue));
                }
            }
        }
        operations.getUpdateExpression(updateReq);
        // AWS doesn't allow empty expressions or attribute collections
        if (!updateReq.UpdateExpression) {
            delete updateReq.UpdateExpression;
        }
        if (!Object.keys(updateReq.ExpressionAttributeNames).length) {
            delete updateReq.ExpressionAttributeNames;
        }
        if (!Object.keys(updateReq.ExpressionAttributeValues).length) {
            delete updateReq.ExpressionAttributeValues;
        }
        var newModel$ = NewModel.$__;
        function updateItem() {
            debug('updateItem', updateReq);
            newModel$.base
                .ddb()
                .updateItem(updateReq, function (err, data) {
                if (err) {
                    debug('Error returned by updateItem', err);
                    return deferred.reject(err);
                }
                debug('updateItem response', data);
                if (!Object.keys(data).length) {
                    return deferred.resolve();
                }
                var model = new NewModel();
                model.$__.isNew = false;
                try {
                    schema.parseDynamo(model, data.Attributes);
                }
                catch (e) {
                    debug('cannot parse data', data);
                    return deferred.reject(e);
                }
                debug('updateItem parsed model', model);
                deferred.resolve(model);
            });
        }
        if (newModel$.options.waitForActive) {
            newModel$.table
                .waitForActive()
                .then(updateItem)
                .catch(deferred.reject);
        }
        else {
            updateItem();
        }
        return deferred.promise.nodeify(next);
    }
    static delete(NewModel, key, options, next) {
        var schema = NewModel.$__.schema;
        var hashKeyName = schema.hashKey && schema.hashKey.name;
        if (!key[hashKeyName]) {
            var keyVal = key;
            key = {};
            key[hashKeyName] = keyVal;
        }
        if (schema.rangeKey && !key[schema.rangeKey.name]) {
            var deferred = Q.defer();
            deferred.reject(new errors_1.ModelError('Range key required: ' + schema.hashKey.name));
            return deferred.promise.nodeify(next);
        }
        var model = new NewModel(key);
        return model.delete(options, next);
    }
    static query(NewModel, query, options, next) {
        if (typeof options === 'function') {
            next = options;
            options = null;
        }
        query = new Query_1.default(NewModel, query, options);
        if (next) {
            query.exec(next);
        }
        return query;
    }
    static queryOne(NewModel, query, options, next) {
        if (typeof options === 'function') {
            next = options;
            options = null;
        }
        query = new Query_1.default(NewModel, query, options);
        query.one();
        if (next) {
            query.exec(next);
        }
        return query;
    }
    static scan(NewModel, filter, options, next) {
        if (typeof options === 'function') {
            next = options;
            options = null;
        }
        var scan = new Scan_1.default(NewModel, filter, options);
        if (next) {
            scan.exec(next);
        }
        return scan;
    }
    static batchGet(NewModel, keys, options, next) {
        debug('BatchGet %j', keys);
        var deferred = Q.defer();
        if (!(keys instanceof Array)) {
            deferred.reject(new errors_1.ModelError('batchGet requires keys to be an array'));
            return deferred.promise.nodeify(next);
        }
        options = options || {};
        if (typeof options === 'function') {
            next = options;
            options = {};
        }
        var schema = NewModel.$__.schema;
        var hashKeyName = schema.hashKey && schema.hashKey.name;
        keys = keys.map(function (key) {
            if (!key[hashKeyName]) {
                var ret = {};
                ret[hashKeyName] = key;
                return ret;
            }
            return key;
        });
        if (schema.rangeKey &&
            !keys.every(function (key) {
                return key[schema.rangeKey.name];
            })) {
            deferred.reject(new errors_1.ModelError('Range key required: ' + schema.rangeKey.name));
            return deferred.promise.nodeify(next);
        }
        var batchReq = { RequestItems: {} };
        var getReq = {};
        batchReq.RequestItems[NewModel.$__.name] = getReq;
        getReq.Keys = keys.map(function (key) {
            var ret = {};
            ret[hashKeyName] =
                schema.hashKey &&
                    schema.hashKey.toDynamo(key[hashKeyName], undefined, key);
            if (schema.rangeKey) {
                var rangeKeyName = schema.rangeKey.name;
                ret[rangeKeyName] = schema.rangeKey.toDynamo(key[rangeKeyName], undefined, key);
            }
            return ret;
        });
        if (options.attributes) {
            getReq.AttributesToGet = options.attributes;
        }
        if (options.consistent) {
            getReq.ConsistentRead = true;
        }
        var newModel$ = NewModel.$__;
        function batchGet() {
            debug('batchGetItem', batchReq);
            newModel$.base
                .ddb()
                .batchGetItem(batchReq, function (err, data) {
                if (err) {
                    debug('Error returned by batchGetItem', err);
                    return deferred.reject(err);
                }
                debug('batchGetItem response', data);
                if (!Object.keys(data).length) {
                    return deferred.resolve();
                }
                function toModel(item) {
                    var model = new NewModel();
                    model.$__.isNew = false;
                    schema.parseDynamo(model, item);
                    debug('batchGet parsed model', model);
                    return model;
                }
                var models = data.Responses[newModel$.name]
                    ? data.Responses[newModel$.name].map(toModel)
                    : [];
                if (data.UnprocessedKeys[newModel$.name]) {
                    // convert unprocessed keys back to dynamoose format
                    models.unprocessed = data.UnprocessedKeys[newModel$.name].Keys.map(function (key) {
                        var ret = {};
                        ret[hashKeyName] =
                            schema.hashKey &&
                                schema.hashKey.parseDynamo(key[hashKeyName]);
                        if (schema.rangeKey) {
                            var rangeKeyName = schema.rangeKey.name;
                            ret[rangeKeyName] = schema.rangeKey.parseDynamo(key[rangeKeyName]);
                        }
                        return ret;
                    });
                }
                deferred.resolve(models);
            });
        }
        if (newModel$.options.waitForActive) {
            newModel$.table
                .waitForActive()
                .then(batchGet)
                .catch(deferred.reject);
        }
        else {
            batchGet();
        }
        return deferred.promise.nodeify(next);
    }
    static batchPut(NewModel, items, options, next) {
        debug('BatchPut %j', items);
        var deferred = Q.defer();
        if (!(items instanceof Array)) {
            deferred.reject(new errors_1.ModelError('batchPut requires items to be an array'));
            return deferred.promise.nodeify(next);
        }
        options = options || {};
        if (typeof options === 'function') {
            next = options;
            options = {};
        }
        var schema = NewModel.$__.schema;
        var newModel$ = NewModel.$__;
        var batchRequests = toBatchChunks(newModel$.name, items, MAX_BATCH_WRITE_SIZE, function (item) {
            return {
                PutRequest: {
                    Item: schema.toDynamo(item),
                },
            };
        });
        var batchPut = function () {
            batchWriteItems(NewModel, batchRequests)
                .then(function (result) {
                deferred.resolve(result);
            })
                .fail(function (err) {
                deferred.reject(err);
            });
        };
        if (newModel$.options.waitForActive) {
            newModel$.table
                .waitForActive()
                .then(batchPut)
                .catch(deferred.reject);
        }
        else {
            batchPut();
        }
        return deferred.promise.nodeify(next);
    }
    static batchDelete(NewModel, keys, options, next) {
        debug('BatchDel %j', keys);
        var deferred = Q.defer();
        if (!(keys instanceof Array)) {
            deferred.reject(new errors_1.ModelError('batchDelete requires keys to be an array'));
            return deferred.promise.nodeify(next);
        }
        options = options || {};
        if (typeof options === 'function') {
            next = options;
            options = {};
        }
        var schema = NewModel.$__.schema;
        var newModel$ = NewModel.$__;
        var hashKeyName = schema.hashKey && schema.hashKey.name;
        var batchRequests = toBatchChunks(newModel$.name, keys, MAX_BATCH_WRITE_SIZE, function (key) {
            var key_element = {};
            (key_element[hashKeyName] =
                schema.hashKey &&
                    schema.hashKey.toDynamo(key[hashKeyName])),
                undefined,
                key;
            if (schema.rangeKey) {
                key_element[schema.rangeKey.name] = schema.rangeKey.toDynamo(key[schema.rangeKey.name], undefined, key);
            }
            return {
                DeleteRequest: {
                    Key: key_element,
                },
            };
        });
        var batchDelete = function () {
            batchWriteItems(NewModel, batchRequests)
                .then(function (result) {
                deferred.resolve(result);
            })
                .fail(function (err) {
                deferred.reject(err);
            });
        };
        if (newModel$.options.waitForActive) {
            newModel$.table
                .waitForActive()
                .then(batchDelete)
                .catch(deferred.reject);
        }
        else {
            batchDelete();
        }
        return deferred.promise.nodeify(next);
    }
}
exports.default = Model;
function toBatchChunks(modelName, list, chunkSize, requestMaker) {
    var listClone = list.slice(0);
    var chunk = [];
    var batchChunks = [];
    while ((chunk = listClone.splice(0, chunkSize)).length) {
        var requests = chunk.map(requestMaker);
        var batchReq = {
            RequestItems: {},
        };
        batchReq.RequestItems[modelName] = requests;
        batchChunks.push(batchReq);
    }
    return batchChunks;
}
function processCondition(req, options, schema) {
    if (options.condition) {
        if (req.ConditionExpression) {
            req.ConditionExpression =
                '(' +
                    req.ConditionExpression +
                    ') and (' +
                    options.condition +
                    ')';
        }
        else {
            req.ConditionExpression = options.condition;
        }
        if (options.conditionNames) {
            req.ExpressionAttributeNames = {};
            for (var name in options.conditionNames) {
                req.ExpressionAttributeNames['#' + name] =
                    options.conditionNames[name];
            }
        }
        if (options.conditionValues) {
            req.ExpressionAttributeValues = {};
            Object.keys(options.conditionValues).forEach(function (k) {
                var val = options.conditionValues[k];
                var attr = schema.attributes[k];
                if (attr) {
                    req.ExpressionAttributeValues[':' + k] = attr.toDynamo(val);
                }
                else {
                    throw new errors_1.ModelError('Invalid condition value: ' +
                        k +
                        '. The name must either be in the schema or a full DynamoDB object must be specified.');
                }
            });
        }
    }
}
function sendErrorToCallback(error, options, next) {
    if (typeof options === 'function') {
        next = options;
    }
    if (typeof next === 'function') {
        next(error);
    }
}
/*!
 * Register methods for this model
 *
 * @param {Model} model
 * @param {Schema} schema
 */
var applyMethods = function (model, schema) {
    debug('applying methods');
    for (var i in schema.methods) {
        model.prototype[i] = schema.methods[i];
    }
};
/*!
 * Register statics for this model
 * @param {Model} model
 * @param {Schema} schema
 */
var applyStatics = function (model, schema) {
    debug('applying statics');
    for (var i in schema.statics) {
        model[i] = schema.statics[i].bind(model);
    }
};
/*!
 * Register virtuals for this model
 * @param {Model} model
 * @param {Schema} schema
 */
var applyVirtuals = function (model, schema) {
    debug('applying virtuals');
    for (var i in schema.virtuals) {
        schema.virtuals[i].applyVirtuals(model);
    }
};
function reduceBatchResult(resultList) {
    return resultList.reduce(function (acc, res) {
        var responses = res.Responses ? res.Responses : {};
        var unprocessed = res.UnprocessedItems ? res.UnprocessedItems : {};
        // merge responses
        for (var tableName in responses) {
            if (responses.hasOwnProperty(tableName)) {
                var consumed = acc.Responses[tableName]
                    ? acc.Responses[tableName].ConsumedCapacityUnits
                    : 0;
                consumed += responses[tableName].ConsumedCapacityUnits;
                acc.Responses[tableName] = {
                    ConsumedCapacityUnits: consumed,
                };
            }
        }
        // merge unprocessed items
        for (var tableName2 in unprocessed) {
            if (unprocessed.hasOwnProperty(tableName2)) {
                var items = acc.UnprocessedItems[tableName2]
                    ? acc.UnprocessedItems[tableName2]
                    : [];
                items.push(unprocessed[tableName2]);
                acc.UnprocessedItems[tableName2] = items;
            }
        }
        return acc;
    }, { Responses: {}, UnprocessedItems: {} });
}
function batchWriteItems(NewModel, batchRequests) {
    debug('batchWriteItems');
    var newModel$ = NewModel.$__;
    var batchList = batchRequests.map(function (batchReq) {
        var deferredBatch = Q.defer();
        newModel$.base
            .ddb()
            .batchWriteItem(batchReq, function (err, data) {
            if (err) {
                debug('Error returned by batchWriteItems', err);
                return deferredBatch.reject(err);
            }
            deferredBatch.resolve(data);
        });
        return deferredBatch.promise;
    });
    return Q.all(batchList).then(function (resultList) {
        return reduceBatchResult(resultList);
    });
}
