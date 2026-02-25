export namespace main {
	
	export class ConnectionRow {
	    id: string;
	    name: string;
	    host: string;
	    port: number;
	    type: string;
	    mode: string;
	    status: string;
	    subline: string;
	
	    static createFrom(source: any = {}) {
	        return new ConnectionRow(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.id = source["id"];
	        this.name = source["name"];
	        this.host = source["host"];
	        this.port = source["port"];
	        this.type = source["type"];
	        this.mode = source["mode"];
	        this.status = source["status"];
	        this.subline = source["subline"];
	    }
	}
	export class ActiveConnectionInfo {
	    hasActive: boolean;
	    connection: ConnectionRow;
	
	    static createFrom(source: any = {}) {
	        return new ActiveConnectionInfo(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.hasActive = source["hasActive"];
	        this.connection = this.convertValues(source["connection"], ConnectionRow);
	    }
	
		convertValues(a: any, classs: any, asMap: boolean = false): any {
		    if (!a) {
		        return a;
		    }
		    if (a.slice && a.map) {
		        return (a as any[]).map(elem => this.convertValues(elem, classs));
		    } else if ("object" === typeof a) {
		        if (asMap) {
		            for (const key of Object.keys(a)) {
		                a[key] = new classs(a[key]);
		            }
		            return a;
		        }
		        return new classs(a);
		    }
		    return a;
		}
	}
	export class SSHInput {
	    enabled: boolean;
	    host: string;
	    port: number;
	    user: string;
	    authType: string;
	    password: string;
	    keyFile: string;
	    passphrase: string;
	
	    static createFrom(source: any = {}) {
	        return new SSHInput(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.enabled = source["enabled"];
	        this.host = source["host"];
	        this.port = source["port"];
	        this.user = source["user"];
	        this.authType = source["authType"];
	        this.password = source["password"];
	        this.keyFile = source["keyFile"];
	        this.passphrase = source["passphrase"];
	    }
	}
	export class ConnectionInput {
	    id: string;
	    name: string;
	    type: string;
	    useConnString: boolean;
	    connString: string;
	    host: string;
	    port: number;
	    database: string;
	    schema: string;
	    username: string;
	    password: string;
	    ssh: SSHInput;
	
	    static createFrom(source: any = {}) {
	        return new ConnectionInput(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.id = source["id"];
	        this.name = source["name"];
	        this.type = source["type"];
	        this.useConnString = source["useConnString"];
	        this.connString = source["connString"];
	        this.host = source["host"];
	        this.port = source["port"];
	        this.database = source["database"];
	        this.schema = source["schema"];
	        this.username = source["username"];
	        this.password = source["password"];
	        this.ssh = this.convertValues(source["ssh"], SSHInput);
	    }
	
		convertValues(a: any, classs: any, asMap: boolean = false): any {
		    if (!a) {
		        return a;
		    }
		    if (a.slice && a.map) {
		        return (a as any[]).map(elem => this.convertValues(elem, classs));
		    } else if ("object" === typeof a) {
		        if (asMap) {
		            for (const key of Object.keys(a)) {
		                a[key] = new classs(a[key]);
		            }
		            return a;
		        }
		        return new classs(a);
		    }
		    return a;
		}
	}
	export class ConnectionPage {
	    items: ConnectionRow[];
	    total: number;
	    page: number;
	    pageSize: number;
	    totalPages: number;
	
	    static createFrom(source: any = {}) {
	        return new ConnectionPage(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.items = this.convertValues(source["items"], ConnectionRow);
	        this.total = source["total"];
	        this.page = source["page"];
	        this.pageSize = source["pageSize"];
	        this.totalPages = source["totalPages"];
	    }
	
		convertValues(a: any, classs: any, asMap: boolean = false): any {
		    if (!a) {
		        return a;
		    }
		    if (a.slice && a.map) {
		        return (a as any[]).map(elem => this.convertValues(elem, classs));
		    } else if ("object" === typeof a) {
		        if (asMap) {
		            for (const key of Object.keys(a)) {
		                a[key] = new classs(a[key]);
		            }
		            return a;
		        }
		        return new classs(a);
		    }
		    return a;
		}
	}
	
	export class DeleteRowRequest {
	    schema: string;
	    table: string;
	    keyColumn: string;
	    keyValue: string;
	
	    static createFrom(source: any = {}) {
	        return new DeleteRowRequest(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.schema = source["schema"];
	        this.table = source["table"];
	        this.keyColumn = source["keyColumn"];
	        this.keyValue = source["keyValue"];
	    }
	}
	export class HistoryItem {
	    id: string;
	    connection_id: string;
	    type: string;
	    query: string;
	    duration_ms: number;
	    error?: string;
	    executed_at: string;
	
	    static createFrom(source: any = {}) {
	        return new HistoryItem(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.id = source["id"];
	        this.connection_id = source["connection_id"];
	        this.type = source["type"];
	        this.query = source["query"];
	        this.duration_ms = source["duration_ms"];
	        this.error = source["error"];
	        this.executed_at = source["executed_at"];
	    }
	}
	export class QueryRunResponse {
	    columns: string[];
	    rows: string[][];
	    rowsAffected: number;
	    message: string;
	    durationMs: number;
	
	    static createFrom(source: any = {}) {
	        return new QueryRunResponse(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.columns = source["columns"];
	        this.rows = source["rows"];
	        this.rowsAffected = source["rowsAffected"];
	        this.message = source["message"];
	        this.durationMs = source["durationMs"];
	    }
	}
	export class RowMutationRequest {
	    schema: string;
	    table: string;
	    keyColumn: string;
	    keyValue: string;
	    row: Record<string, string>;
	
	    static createFrom(source: any = {}) {
	        return new RowMutationRequest(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.schema = source["schema"];
	        this.table = source["table"];
	        this.keyColumn = source["keyColumn"];
	        this.keyValue = source["keyValue"];
	        this.row = source["row"];
	    }
	}
	
	export class SnippetItem {
	    id: string;
	    name: string;
	    connection: string;
	    type: string;
	    query: string;
	    created_at: string;
	    updated_at: string;
	
	    static createFrom(source: any = {}) {
	        return new SnippetItem(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.id = source["id"];
	        this.name = source["name"];
	        this.connection = source["connection"];
	        this.type = source["type"];
	        this.query = source["query"];
	        this.created_at = source["created_at"];
	        this.updated_at = source["updated_at"];
	    }
	}
	export class TableDataRequest {
	    schema: string;
	    table: string;
	    filter: string;
	    sort: string;
	    limit: number;
	    page: number;
	
	    static createFrom(source: any = {}) {
	        return new TableDataRequest(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.schema = source["schema"];
	        this.table = source["table"];
	        this.filter = source["filter"];
	        this.sort = source["sort"];
	        this.limit = source["limit"];
	        this.page = source["page"];
	    }
	}
	export class TableDataResponse {
	    schema: string;
	    table: string;
	    columns: string[];
	    rows: string[][];
	    page: number;
	    pageSize: number;
	    hasNext: boolean;
	    from: number;
	    to: number;
	
	    static createFrom(source: any = {}) {
	        return new TableDataResponse(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.schema = source["schema"];
	        this.table = source["table"];
	        this.columns = source["columns"];
	        this.rows = source["rows"];
	        this.page = source["page"];
	        this.pageSize = source["pageSize"];
	        this.hasNext = source["hasNext"];
	        this.from = source["from"];
	        this.to = source["to"];
	    }
	}

}

export namespace model {
	
	export class DBObject {
	    schema: string;
	    name: string;
	    type: string;
	
	    static createFrom(source: any = {}) {
	        return new DBObject(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.schema = source["schema"];
	        this.name = source["name"];
	        this.type = source["type"];
	    }
	}
	export class TableColumn {
	    name: string;
	    dataType: string;
	    nullable: boolean;
	    defaultValue?: string;
	    isPrimary: boolean;
	    enumValues?: string[];
	
	    static createFrom(source: any = {}) {
	        return new TableColumn(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.name = source["name"];
	        this.dataType = source["dataType"];
	        this.nullable = source["nullable"];
	        this.defaultValue = source["defaultValue"];
	        this.isPrimary = source["isPrimary"];
	        this.enumValues = source["enumValues"];
	    }
	}
	export class TableInfo {
	    schema: string;
	    name: string;
	    rows: number;
	    size: string;
	    last_updated: string;
	
	    static createFrom(source: any = {}) {
	        return new TableInfo(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.schema = source["schema"];
	        this.name = source["name"];
	        this.rows = source["rows"];
	        this.size = source["size"];
	        this.last_updated = source["last_updated"];
	    }
	}

}

