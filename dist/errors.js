"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
class SchemaError extends Error {
    constructor(message) {
        super(message || 'Error with Schema');
    }
}
exports.SchemaError = SchemaError;
class ModelError extends Error {
    constructor(message) {
        super(message || 'Error with Model');
    }
}
exports.ModelError = ModelError;
class QueryError extends Error {
    constructor(message) {
        super(message || 'Error with Query');
    }
}
exports.QueryError = QueryError;
class ScanError extends Error {
    constructor(message, obj) {
        super(message || 'Error with Scan');
        this.obj = obj;
    }
}
exports.ScanError = ScanError;
class ValidationError extends Error {
    constructor(message) {
        super(message || 'Validation Error');
    }
}
exports.ValidationError = ValidationError;
class ParseError extends Error {
    constructor(message, originalError) {
        super(message || 'Parse Error');
        this.originalError = originalError.message;
    }
}
exports.ParseError = ParseError;
