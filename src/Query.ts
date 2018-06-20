import * as Q from 'q';
import ModelClass from './Model';
import Schema from './Schema';
import Attribute from './Attribute';
import { QueryError } from './errors';
import * as Debug from 'debug';
import { DynamoDB } from 'aws-sdk';

const debug = Debug('dynamoose:query');

const VALID_RANGE_KEYS = [
    'EQ',
    'LE',
    'LT',
    'GE',
    'GT',
    'BEGINS_WITH',
    'BETWEEN',
];

interface IFilter {
    name?: string;
    values?: any[] | null;
    comparison?: string;
}

interface IQueryReq {
    TableName: string;
    KeyConditions: { [key: string]: any };
    ScanFilter?: { [key: string]: any };
    ConsistentRead?: boolean;
    AttributesToGet?: string[];
    Select?: string;
    Limit?: number;
    TotalSegments?: number;
    Segment?: number;
    ConditionalOperator?: 'AND' | 'OR';
    ExclusiveStartKey?: { [key: string]: any };
    IndexName?: string;
    QueryFilter?: {
        [key: string]: {
            AttributeValueList: any[];
            ComparisonOperator: string;
        };
    };
    ScanIndexForward?: boolean;
}

interface IQueryOptions {
    conditionalOperator?: 'AND' | 'OR';
    consistent?: boolean;
    limit?: number;
    ExclusiveStartKey?: { [key: string]: any } | Array<{ [key: string]: any }>;
    count?: boolean;
    counts?: boolean;
    select?: string;
    parallel?: number;
    attributes?: string[];
    all: { delay: number; max: number };
    one?: boolean;
    indexName?: string;
    descending?: boolean;
    or?: boolean;
}

interface IQuery {
    [key: string]: any;
    hashKey: { name: string; value: any };
    hash?: { [key: string]: any };
    range?: { [key: string]: any };
    rangeKey: {
        name?: string;
        values?: any[] | null;
        comparison?: string;
    };
}

export default class Query {
    buildState: false | string = false;
    validationError: null | QueryError = null;
    query: IQuery = { hashKey: { name: '', value: '' }, rangeKey: {} };
    notState: boolean = false;
    currentFilter: string = '';
    filters: { [key: string]: IFilter } = {};
    constructor(
        public Model: ModelClass,
        query: string | IQuery = {
            hashKey: { name: '', value: '' },
            rangeKey: {},
        },
        public options: IQueryOptions = { all: { delay: 0, max: 1 } },
    ) {
        let hashKeyName, hashKeyVal;
        if (typeof query === 'string') {
            this.buildState = 'hashKey';
            this.query.hashKey.name = query;
        } else if (query.hash) {
            hashKeyName = Object.keys(query.hash)[0];
            hashKeyVal = query.hash[hashKeyName];
            if (hashKeyVal.eq !== null && hashKeyVal.eq !== undefined) {
                hashKeyVal = hashKeyVal.eq;
            }
            this.query.hashKey.name = hashKeyName;
            this.query.hashKey.value = hashKeyVal;

            if (query.range) {
                var rangeKeyName = Object.keys(query.range)[0];
                var rangeKeyVal = query.range[rangeKeyName];
                var rangeKeyComp = Object.keys(rangeKeyVal)[0];
                rangeKeyVal = rangeKeyVal[rangeKeyComp];
                this.query.rangeKey = {
                    name: rangeKeyName,
                    values: [rangeKeyVal],
                    comparison: rangeKeyComp,
                };
            }
        } else {
            hashKeyName = Object.keys(query)[0];
            hashKeyVal = query[hashKeyName];
            if (hashKeyVal.eq !== null && hashKeyVal.eq !== undefined) {
                hashKeyVal = hashKeyVal.eq;
            }
            this.query.hashKey.name = hashKeyName;
            this.query.hashKey.value = hashKeyVal;
        }
    }

    exec(next: ((err: Error, data?: any) => void)) {
        debug('exec query for ', this.query);
        if (this.validationError) {
            if (next) {
                next(this.validationError);
            }
            return Q.reject(this.validationError);
        }

        const Model = this.Model;
        const Model$ = Model.$__;
        const schema = Model$.schema;
        const options = this.options;

        debug('Query with schema', schema);

        let queryReq: IQueryReq = {
            TableName: Model.$__.name,
            KeyConditions: {},
        };

        let indexName: string, index;
        // Check both hash key and range key in the query to see if they do not match
        // the hash and range key on the primary table.  If they don't match then we
        // can look for a secondary index to query.
        if (
            (schema.hashKey &&
                schema.hashKey.name !== this.query.hashKey.name) ||
            (this.query.rangeKey &&
                schema.rangeKey &&
                schema.rangeKey.name !== this.query.rangeKey.name)
        ) {
            debug('query is on global secondary index');
            for (indexName in schema.indexes.global) {
                index = schema.indexes.global[indexName];
                if (index.name === this.query.hashKey.name) {
                    debug('using index', indexName);
                    queryReq.IndexName = indexName;
                    break;
                }
            }
        }

        var hashAttr = schema.attributes[this.query.hashKey.name];

        queryReq.KeyConditions[this.query.hashKey.name] = {
            AttributeValueList: [
                hashAttr.toDynamo(
                    this.query.hashKey.value,
                    this.query.hashKey.name,
                ),
            ],
            ComparisonOperator: 'EQ',
        };

        var i, val: any;

        if (this.query.rangeKey) {
            var rangeKey = this.query.rangeKey;
            var rangeAttr: Attribute =
                schema.attributes[rangeKey.name as string];

            if (
                !queryReq.IndexName &&
                schema.rangeKey &&
                schema.rangeKey.name !== rangeKey.name
            ) {
                debug('query is on local secondary index');
                for (indexName in schema.indexes.local) {
                    index = schema.indexes.local[indexName];
                    if (index.name === rangeKey.name) {
                        debug('using local index', indexName);
                        queryReq.IndexName = indexName;
                        break;
                    }
                }
            }

            if (!rangeKey || rangeKey.values === undefined) {
                debug('No range key value (i.e. get all)');
            } else {
                debug('Range key: %s', rangeKey.name);
                var keyConditions: any = (queryReq.KeyConditions[
                    rangeKey.name as string
                ] = {
                    AttributeValueList: [],
                    ComparisonOperator: (rangeKey.comparison as string).toUpperCase(),
                });
                for (i = 0; i < (rangeKey.values as any[]).length; i++) {
                    val = (rangeKey.values as any[])[i];
                    keyConditions.AttributeValueList.push(
                        rangeAttr.toDynamo(val, true),
                    );
                }
            }
        }

        // if the index name has been explicitly set via the api then let that override
        // anything that has been previously derived
        if (this.options.indexName) {
            debug('forcing index: %s', this.options.indexName);
            queryReq.IndexName = this.options.indexName;
        }

        if (this.filters && Object.keys(this.filters).length > 0) {
            queryReq.QueryFilter = {};
            for (var name in this.filters) {
                debug('Filter on: %s', name);
                var filter = this.filters[name];
                var filterAttr = schema.attributes[name];
                queryReq.QueryFilter[name] = {
                    AttributeValueList: [],
                    ComparisonOperator: (filter.comparison as string).toUpperCase(),
                };

                var isContains =
                    filter.comparison === 'CONTAINS' ||
                    filter.comparison === 'NOT_CONTAINS';
                var isListContains =
                    isContains && filterAttr.type.name === 'list';

                if (filter.values) {
                    for (i = 0; i < filter.values.length; i++) {
                        val = filter.values[i];
                        queryReq.QueryFilter[name].AttributeValueList.push(
                            isListContains
                                ? filterAttr.attributes[0].toDynamo(val, true)
                                : filterAttr.toDynamo(val, true),
                        );
                    }
                }
            }
        }

        if (options.or) {
            queryReq.ConditionalOperator = 'OR'; // defualts to AND
        }

        if (options.attributes) {
            queryReq.AttributesToGet = options.attributes;
        }

        if (options.count) {
            queryReq.Select = 'COUNT';
        }

        if (options.counts) {
            queryReq.Select = 'COUNT';
        }

        if (options.consistent) {
            queryReq.ConsistentRead = true;
        }

        if (options.limit) {
            queryReq.Limit = options.limit;
        }

        if (options.one) {
            queryReq.Limit = 1;
        }

        if (options.descending) {
            queryReq.ScanIndexForward = false;
        }

        if (options.ExclusiveStartKey) {
            queryReq.ExclusiveStartKey = options.ExclusiveStartKey;
        }

        function query() {
            var deferred = Q.defer();

            if (!options.all) {
                options.all = { delay: 0, max: 1 };
            }

            var models: { [key: string]: any } = {},
                totalCount = 0,
                scannedCount = 0,
                timesQueried = 0,
                lastKey: DynamoDB.Key | undefined;
            queryOne();

            function queryOne() {
                debug('DynamoDB Query: %j', queryReq);
                Model$.base.ddb().query(queryReq, function(err, data) {
                    if (err) {
                        debug('Error returned by query', err);
                        return deferred.reject(err);
                    }
                    debug('DynamoDB Query Response: %j', data);

                    if (!Object.keys(data).length) {
                        return deferred.resolve();
                    }

                    function toModel(item: any) {
                        var model = new ModelClass();
                        model.$__.isNew = false;
                        schema.parseDynamo(model, item);

                        debug('query parsed model', model);

                        return model;
                    }

                    try {
                        if (options.count) {
                            return deferred.resolve(data.Count);
                        }
                        if (options.counts) {
                            var counts = {
                                count: data.Count,
                                scannedCount: data.ScannedCount,
                            };
                            return deferred.resolve(counts);
                        }
                        if (data.Items !== undefined) {
                            if (!models.length) {
                                models = data.Items.map(toModel);
                            } else {
                                models = models.concat(data.Items.map(toModel));
                            }

                            if (options.one) {
                                if (!models || models.length === 0) {
                                    return deferred.resolve();
                                }
                                return deferred.resolve(models[0]);
                            }
                            lastKey = data.LastEvaluatedKey;
                        }
                        totalCount += data.Count as number;
                        scannedCount += data.ScannedCount as number;
                        timesQueried++;

                        if (
                            (options.all.max === 0 ||
                                timesQueried < options.all.max) &&
                            lastKey
                        ) {
                            // query.all need to query again
                            queryReq.ExclusiveStartKey = lastKey;
                            setTimeout(queryOne, options.all.delay);
                        } else {
                            models.lastKey = lastKey;
                            models.count = totalCount;
                            models.scannedCount = scannedCount;
                            models.timesQueried = timesQueried;
                            deferred.resolve(models);
                        }
                    } catch (err) {
                        deferred.reject(err);
                    }
                });
            }

            return deferred.promise.nodeify(next);
        }

        if (Model$.options.waitForActive) {
            return Model$.table
                .waitForActive()
                .then(query)
                .catch(query);
        }

        return query();
    }

    where(rangeKey: string | { [key: string]: any }) {
        if (this.validationError) {
            return this;
        }
        if (this.buildState) {
            this.validationError = new QueryError(
                'Invalid Query state: where() must follow eq()',
            );
            return this;
        }
        if (typeof rangeKey === 'string') {
            this.buildState = 'rangeKey';
            this.query.rangeKey = { name: rangeKey };
        } else {
            var rangeKeyName = Object.keys(rangeKey)[0];
            var rangeKeyVal = rangeKey[rangeKeyName];
            var rangeKeyComp = Object.keys(rangeKeyVal)[0];
            rangeKeyVal = rangeKeyVal[rangeKeyComp];
            this.query.rangeKey = {
                name: rangeKeyName,
                values: [rangeKeyVal],
                comparison: rangeKeyComp,
            };
        }

        return this;
    }

    filter(filter: string) {
        if (this.validationError) {
            return this;
        }
        if (this.buildState) {
            this.validationError = new QueryError(
                'Invalid Query state: filter() must follow comparison',
            );
            return this;
        }
        if (typeof filter === 'string') {
            this.buildState = 'filter';
            this.currentFilter = filter;
            if (this.filters[filter]) {
                this.validationError = new QueryError(
                    `Invalid Query state: %{filter} filter can only be used once`,
                );
                return this;
            }
            this.filters[filter] = { name: filter };
        }

        return this;
    }

    compVal(vals: any[] | null, comp: string) {
        if (this.validationError) {
            return this;
        }
        if (this.buildState === 'hashKey') {
            if (comp !== 'EQ') {
                this.validationError = new QueryError(
                    'Invalid Query state: eq must follow query()',
                );
                return this;
            }
            this.query.hashKey.value = vals && vals[0];
        } else if (this.buildState === 'rangeKey') {
            if (VALID_RANGE_KEYS.indexOf(comp) < 0) {
                this.validationError = new QueryError(
                    `Invalid Query state: ${comp} must follow filter()`,
                );
                return this;
            }
            this.query.rangeKey.values = vals;
            this.query.rangeKey.comparison = comp;
        } else if (this.buildState === 'filter') {
            this.filters[this.currentFilter].values = vals;
            this.filters[this.currentFilter].comparison = comp;
        } else {
            this.validationError = new QueryError(
                `Invalid Query state: ${comp} must follow query(), where() or filter()`,
            );
            return this;
        }

        this.buildState = false;
        this.notState = false;

        return this;
    }

    and() {
        this.options.or = false;

        return this;
    }

    or() {
        this.options.or = true;

        return this;
    }

    not() {
        this.notState = true;
        return this;
    }

    null() {
        if (this.notState) {
            return this.compVal(null, 'NOT_NULL');
        } else {
            return this.compVal(null, 'NULL');
        }
    }

    eq(val: any) {
        if (this.notState) {
            return this.compVal([val], 'NE');
        } else {
            return this.compVal([val], 'EQ');
        }
    }

    lt(val: any) {
        if (this.notState) {
            return this.compVal([val], 'GE');
        } else {
            return this.compVal([val], 'LT');
        }
    }

    le(val: any) {
        if (this.notState) {
            return this.compVal([val], 'GT');
        } else {
            return this.compVal([val], 'LE');
        }
    }

    ge(val: any) {
        if (this.notState) {
            return this.compVal([val], 'LT');
        } else {
            return this.compVal([val], 'GE');
        }
    }

    gt(val: any) {
        if (this.notState) {
            return this.compVal([val], 'LE');
        } else {
            return this.compVal([val], 'GT');
        }
    }

    contains(val: string) {
        if (this.notState) {
            return this.compVal([val], 'NOT_CONTAINS');
        } else {
            return this.compVal([val], 'CONTAINS');
        }
    }

    beginsWith(val: string) {
        if (this.validationError) {
            return this;
        }
        if (this.notState) {
            this.validationError = new QueryError(
                'Invalid Query state: beginsWith() cannot follow not()',
            );
            return this;
        }
        return this.compVal([val], 'BEGINS_WITH');
    }

    in(vals: any[]) {
        if (this.validationError) {
            return this;
        }
        if (this.notState) {
            this.validationError = new QueryError(
                'Invalid Query state: in() cannot follow not()',
            );
            return this;
        }

        return this.compVal(vals, 'IN');
    }

    between(a: any, b: any) {
        if (this.validationError) {
            return this;
        }
        if (this.notState) {
            this.validationError = new QueryError(
                'Invalid Query state: between() cannot follow not()',
            );
            return this;
        }
        return this.compVal([a, b], 'BETWEEN');
    }

    limit(limit: number) {
        this.options.limit = limit;
        return this;
    }

    one() {
        this.options.one = true;
        return this;
    }

    consistent() {
        this.options.consistent = true;
        return this;
    }

    descending() {
        this.options.descending = true;
        return this;
    }

    ascending() {
        this.options.descending = false;
        return this;
    }

    startAt(key: { [key: string]: any }) {
        this.options.ExclusiveStartKey = key;
        return this;
    }

    attributes(attributes: string[]) {
        this.options.attributes = attributes;
        return this;
    }

    count() {
        this.options.count = true;
        this.options.select = 'COUNT';
        return this;
    }

    counts() {
        this.options.counts = true;
        this.options.select = 'COUNT';
        return this;
    }

    using(indexName: string) {
        this.options.indexName = indexName;
        return this;
    }

    all(delay: number = 1000, max: number = 0) {
        this.options.all = { delay: delay, max: max };
        return this;
    }
}
