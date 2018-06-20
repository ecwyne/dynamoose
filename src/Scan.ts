import * as Q from 'q';
import * as _ from 'lodash';
import ModelClass from './Model';
import { ScanError } from './errors';
import * as Debug from 'debug';
const debug = Debug('dynamoose:scan');

interface IScanReq {
    TableName: string;
    ScanFilter?: { [key: string]: any };
    ConsistentRead?: boolean;
    AttributesToGet?: string[];
    Select?: string;
    Limit?: number;
    TotalSegments?: number;
    Segment?: number;
    ConditionalOperator?: 'AND' | 'OR';
    ExclusiveStartKey: { [key: string]: any };
}

interface IScanOptions {
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
}

export default class Scan {
    filters: { [key: string]: any } = {};
    buildState: false | string = false;
    validationError: ScanError | null = null;
    notState: boolean = false;
    constructor(
        public Model: ModelClass,
        filter: string | any,
        public options: IScanOptions = { all: { delay: 0, max: 1 } },
    ) {
        if (typeof filter === 'string') {
            this.buildState = filter;
            this.filters[filter] = { name: filter };
        } else if (typeof filter === 'object') {
            if (typeof filter.FilterExpression === 'string') {
                // if filter expression is given, just assign the filter
                this.filters = filter;
            } else {
                this.parseFilterObject(filter);
            }
        }
    }

    exec(next: ((err: any, data?: any) => void)) {
        debug('exec scan for ', this);
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

        let scanReq: IScanReq = { TableName: '', ExclusiveStartKey: {} };

        function toModel(item: any) {
            const model = new ModelClass();
            model.$__.isNew = false;
            schema && schema.parseDynamo(model, item);

            debug('scan parsed model', model);

            return model;
        }

        function scanByRawFilter() {
            const deferred = Q.defer();
            const dbClient = Model.$__.base.documentClient();
            const DynamoDBSet = dbClient.createSet([1, 2, 3]).constructor;

            dbClient.scan(scanReq, function(err: Error, data: any) {
                if (err) {
                    return deferred.reject(err);
                } else {
                    if (!data) {
                        return deferred.resolve([]);
                    }
                    if (!data.Items) {
                        var counts = {
                            count: data.Count,
                            scannedCount: data.ScannedCount,
                        };
                        return deferred.resolve(counts);
                    }
                    return deferred.resolve(
                        data.Items.map(function(item: any) {
                            var model;

                            Object.keys(item).forEach(function(prop) {
                                if (item[prop] instanceof DynamoDBSet) {
                                    item[prop] = item[prop].values;
                                }
                            });

                            model = new ModelClass(item);
                            model.$__.isNew = false;
                            debug('scan parsed model', model);
                            return model;
                        }),
                    );
                }
            });

            return deferred.promise.nodeify(next);
        }

        if (this.filters && typeof this.filters.FilterExpression === 'string') {
            // use the raw aws filter, which needs to be composed by the developer
            scanReq = this.filters as IScanReq;
            if (!scanReq.TableName) {
                scanReq.TableName = Model.$__.name;
            }

            // use the document client in aws-sdk
            return scanByRawFilter();
        } else {
            // default
            scanReq = {
                TableName: Model.$__.name,
                ExclusiveStartKey: {},
            };

            if (Object.keys(this.filters).length > 0) {
                scanReq.ScanFilter = {};
                for (const name in this.filters) {
                    const filter = this.filters[name];
                    const filterAttr = schema && schema.attributes[name];
                    scanReq.ScanFilter[name] = {
                        AttributeValueList: [],
                        ComparisonOperator: filter.comparison,
                    };

                    const isContains =
                        filter.comparison === 'CONTAINS' ||
                        filter.comparison === 'NOT_CONTAINS';
                    const isListContains =
                        isContains &&
                        filterAttr &&
                        filterAttr.type.name === 'list';

                    if (filter.values) {
                        for (var i = 0; i < filter.values.length; i++) {
                            var val = filter.values[i];
                            scanReq.ScanFilter[name].AttributeValueList.push(
                                isListContains
                                    ? filterAttr.attributes[0].toDynamo(
                                          val,
                                          true,
                                      )
                                    : filterAttr.toDynamo(val, true),
                            );
                        }
                    }
                }
            }

            if (options.attributes) {
                scanReq.AttributesToGet = options.attributes;
            }

            if (options.count) {
                scanReq.Select = 'COUNT';
            }

            if (options.counts) {
                scanReq.Select = 'COUNT';
            }

            if (options.limit) {
                scanReq.Limit = options.limit;
            }

            if (options.parallel) {
                scanReq.TotalSegments = options.parallel;
            }

            if (Array.isArray(options.ExclusiveStartKey)) {
                scanReq.TotalSegments = options.ExclusiveStartKey.length;
            } else if (options.ExclusiveStartKey) {
                options.ExclusiveStartKey = [options.ExclusiveStartKey];
            }

            if (options.conditionalOperator) {
                scanReq.ConditionalOperator = options.conditionalOperator;
            }

            if (options.consistent) {
                scanReq.ConsistentRead = true;
            }
        }

        function scanSegment(segment: number) {
            var deferred = Q.defer();

            var scanOneReq: IScanReq = _.clone(scanReq);

            if (scanOneReq.TotalSegments) {
                scanOneReq.Segment = segment;
            }

            if (options.ExclusiveStartKey) {
                scanOneReq.ExclusiveStartKey = (options.ExclusiveStartKey as {
                    [key: string]: any;
                })[segment];
            }

            debug('adding scan segement', scanOneReq);

            let models: Array<ModelClass> = [],
                totalCount = 0,
                scannedCount = 0,
                timesScanned = 0,
                lastKey: { [key: string]: any };
            if (!options.all) {
                options.all = { delay: 0, max: 1 };
            }
            scanOne();
            function scanOne() {
                debug('scan request', scanOneReq);
                Model.$__.base
                    .ddb()
                    .scan(scanOneReq, function(err: Error, data: any) {
                        if (err) {
                            debug('Error returned by scan', err);
                            return deferred.reject(err);
                        }
                        debug('scan response', data);

                        if (!Object.keys(data).length) {
                            return deferred.resolve();
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
                                if (!models.length || models.length === 0) {
                                    models = data.Items.map(toModel);
                                } else {
                                    models = models.concat(
                                        data.Items.map(toModel),
                                    );
                                }

                                if (options.one) {
                                    if (!models || models.length === 0) {
                                        return deferred.resolve();
                                    }
                                    return deferred.resolve(models[0]);
                                }
                                lastKey = data.LastEvaluatedKey;
                            }
                            totalCount += data.Count;
                            scannedCount += data.ScannedCount;
                            timesScanned++;

                            if (
                                (options.all.max === 0 ||
                                    timesScanned < options.all.max) &&
                                lastKey
                            ) {
                                // scan.all need to scan again
                                scanOneReq.ExclusiveStartKey = lastKey;
                                setTimeout(scanOne, options.all.delay);
                            } else {
                                // completed scan returning models
                                (models as any).lastKey = lastKey;
                                (models as any).count = totalCount;
                                (models as any).scannedCount = scannedCount;
                                (models as any).timesScanned = timesScanned;
                                deferred.resolve(models);
                            }
                        } catch (err) {
                            deferred.reject(err);
                        }
                    });
            }

            return deferred.promise;
        }

        function scan() {
            var deferred = Q.defer();

            var totalSegments = scanReq.TotalSegments || 1;
            var scans = [];
            for (var segment = 0; segment < totalSegments; segment++) {
                scans.push(scanSegment(segment));
            }
            Q.all(scans)
                .then(function(results) {
                    var models = _.flatten(results);
                    var lastKeys = results.map((r: any) => r.lastKey);

                    if (lastKeys.length === 1) {
                        (models as any).lastKey = lastKeys[0];
                    } else if (_.compact(lastKeys).length !== 0) {
                        (models as any).lastKey = lastKeys;
                    }

                    (models as any).count = results.reduce(function(
                        acc,
                        r: any,
                    ) {
                        return acc + r.count;
                    },
                    0);
                    (models as any).scannedCount = results.reduce(function(
                        acc,
                        r: any,
                    ) {
                        return acc + r.scannedCount;
                    },
                    0);
                    (models as any).timesScanned = results.reduce(function(
                        acc,
                        r: any,
                    ) {
                        return acc + r.timesScanned;
                    },
                    0);
                    deferred.resolve(models);
                })
                .fail(function(err) {
                    deferred.reject(err);
                });

            return deferred.promise.nodeify(next);
        }

        if (Model$.options.waitForActive) {
            return Model$.table
                .waitForActive()
                .then(scan)
                .catch(function(err: Error) {
                    if (next) {
                        next(err);
                    }
                    return Q.reject(err);
                });
        }

        return scan();
    }

    parseFilterObject(filter: { [key: string]: any }) {
        if (Object.keys(filter).length > 0) {
            for (var filterName in filter) {
                if (filter.hasOwnProperty(filterName)) {
                    // Parse AND OR
                    if (filterName === 'and' || filterName === 'or') {
                        this[filterName]();
                        for (var condition in filter[filterName]) {
                            if (filter[filterName].hasOwnProperty(condition)) {
                                this.parseFilterObject(
                                    filter[filterName][condition],
                                );
                            }
                        }
                    } else {
                        this.where(filterName);
                        var val, comp;

                        if (
                            typeof filter[filterName] === 'object' &&
                            Object.keys(filter[filterName]).length === 1
                        ) {
                            comp = Object.keys(filter[filterName])[0];

                            if (comp === 'null') {
                                if (!filter[filterName][comp]) {
                                    comp = 'not_null';
                                }
                                val = [null];
                            } else if (comp === 'in' || comp === 'between') {
                                val = filter[filterName][comp];
                            } else {
                                val = [filter[filterName][comp]];
                            }
                        } else {
                            comp = 'eq';
                            val = [filter[filterName]];
                        }
                        this.compVal(val, comp.toUpperCase());
                    }
                }
            }
        }
    }

    and() {
        this.options.conditionalOperator = 'AND';
        return this;
    }
    or() {
        this.options.conditionalOperator = 'OR';
        return this;
    }
    consistent() {
        this.options.consistent = true;
        return this;
    }

    where(filter: string | any) {
        if (this.validationError) {
            return this;
        }

        if (this.buildState) {
            this.validationError = new ScanError(
                'Invalid scan state; where() must follow comparison',
            );
            return this;
        }
        if (typeof filter === 'string') {
            this.buildState = filter;
            if (this.filters[filter]) {
                this.validationError = new ScanError(
                    'Invalid scan state; %s can only be used once',
                    filter,
                );
                return this;
            }
            this.filters[filter] = { name: filter };
        }

        return this;
    }
    filter = this.where;

    compVal(vals: any[] | null, comp: string) {
        if (this.validationError) {
            return this;
        }

        var permittedComparison = [
            'NOT_NULL',
            'NULL',
            'EQ',
            'NE',
            'GE',
            'LT',
            'GT',
            'LE',
            'GE',
            'NOT_CONTAINS',
            'CONTAINS',
            'BEGINS_WITH',
            'IN',
            'BETWEEN',
        ];

        if (!this.buildState) {
            this.validationError = new ScanError(
                'Invalid scan state; %s must follow scan(), where(), or filter()',
                comp,
            );
            return this;
        }

        if (permittedComparison.indexOf(comp) === -1) {
            this.validationError = new ScanError('Invalid comparison %s', comp);
            return this;
        }

        this.filters[this.buildState].values = vals;
        this.filters[this.buildState].comparison = comp;

        this.buildState = false;
        this.notState = false;

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
        if (val === '' || val === null || val === undefined) {
            return this.null();
        }
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

    contains(val: any) {
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
            this.validationError = new ScanError(
                'Invalid scan state: beginsWith() cannot follow not()',
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
            this.validationError = new ScanError(
                'Invalid scan state: in() cannot follow not()',
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
            this.validationError = new ScanError(
                'Invalid scan state: between() cannot follow not()',
            );
            return this;
        }
        return this.compVal([a, b], 'BETWEEN');
    }

    limit(limit: number) {
        this.options.limit = limit;
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

    all(delay: number = 1000, max: number = 0) {
        this.options.all = { delay: delay, max: max };
        return this;
    }

    parallel(numberOfSegments: number) {
        this.options.parallel = numberOfSegments;
        return this;
    }
}
