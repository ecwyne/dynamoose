import * as Debug from 'debug';

const debug = Debug('dynamoose:virtualtype');

export default class VirtualType {
    setter?: Function;
    getter?: Function;
    constructor(
        private options: { [key: string]: any } = {},
        private path: string,
    ) {}

    get(fn: Function) {
        debug('registering getter for ' + this.path);
        this.getter = fn;
        return this;
    }
    set(fn: Function) {
        debug('registering setter for ' + this.path);
        this.setter = fn;
        return this;
    }
    applyVirtuals(model: any) {
        debug('applyVirtuals for %s', this.path);
        const property: { [key: string]: any } = {
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
