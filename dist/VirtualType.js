"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const Debug = require("debug");
const debug = Debug('dynamoose:virtualtype');
class VirtualType {
    constructor(options = {}, path) {
        this.options = options;
        this.path = path;
    }
    get(fn) {
        debug('registering getter for ' + this.path);
        this.getter = fn;
        return this;
    }
    set(fn) {
        debug('registering setter for ' + this.path);
        this.setter = fn;
        return this;
    }
    applyVirtuals(model) {
        debug('applyVirtuals for %s', this.path);
        const property = {
            enumerable: true,
            configurable: true,
        };
        if (this.setter) {
            property.set = this.setter;
        }
        if (this.getter) {
            property.get = this.getter;
        }
        Object.defineProperty(model, this.path, property);
    }
}
exports.default = VirtualType;
