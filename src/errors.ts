export class SchemaError extends Error {
    constructor(message: string) {
        super(message || 'Error with Schema');
    }
}

export class ModelError extends Error {
    constructor(message: string) {
        super(message || 'Error with Model');
    }
}

export class QueryError extends Error {
    constructor(message: string) {
        super(message || 'Error with Query');
    }
}

export class ScanError extends Error {
    constructor(message: string, public obj?: any) {
        super(message || 'Error with Scan');
    }
}

export class ValidationError extends Error {
    constructor(message: string) {
        super(message || 'Validation Error');
    }
}

export class ParseError extends Error {
    originalError: string;
    constructor(message: string, originalError: Error) {
        super(message || 'Parse Error');
        this.originalError = originalError.message;
    }
}
