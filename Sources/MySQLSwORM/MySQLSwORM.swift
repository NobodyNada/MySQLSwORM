import SwORM
import MySQL
import Serializer

extension MySQLDatabase: SwORM.Database {
    public func _newConnection(on worker: Worker) -> EventLoopFuture<SwORM.Connection> {
        return newConnection(on: worker).map { MySQLConnectionWrapper(database: self, connection: $0) }
    }
    
    public var dialect: SQLDialect { return .mysql }
    
    private struct Version: DatabaseObject {
        static let tableName: String = "_sworm_version"
        static let primaryKey: KeyPath<MySQLDatabase.Version, Int64?> = \.id
        
        var id: Int64?
        var version: Int
        
        static let allColumns: [(PartialKeyPath<MySQLDatabase.Version>, CodingKey)] = [
            (\Version.id, CodingKeys.id),
            (\Version.version, CodingKeys.version),
            ]
    }
    
    private func createVersionTable(connection: SwORM.Connection) -> Future<SwORM.Connection> {
        return connection
            .execute("CREATE TABLE IF NOT EXISTS \(Version.tableName) (id INTEGER PRIMARY KEY NOT NULL, version INTEGER NOT NULL)")
            .map { _ in connection }
    }
    
    public func loadSchemaVersion(on worker: Worker) -> EventLoopFuture<Int> {
        return connection(on: worker)
            .then(createVersionTable)
            .then { Version.find(0, connection: $0) }
            .map { $0?.version ?? 0 }
    }
    
    public func setSchemaVersion(to version: Int, on worker: Worker) -> EventLoopFuture<Void> {
        let statement = "REPLACE INTO \(Version.tableName) (id, version) VALUES (0, ?)"
        return connection(on: worker)
            .then(createVersionTable)
            .then { connection in connection.execute(statement, parameters: version) }
            .map(to: Void.self) { _ in }
    }
}

extension DatabaseNativeType: Encodable, MySQLDataConvertible {
    public func convertToMySQLData() -> MySQLData {
        switch self {
        case .int(let v):
            return v.convertToMySQLData()
        case .double(let v):
            return v.convertToMySQLData()
        case .string(let v):
            return v.convertToMySQLData()
        case .data(let v):
            return .init(data: v)
        case .date(let v):
            return v.convertToMySQLData()
        case .null:
            return .null
        }
    }
    
    public static func convertFromMySQLData(_ mysqlData: MySQLData) throws -> DatabaseNativeType {
        return try mysqlData.toNative()
    }
    
    public func encode(to encoder: Encoder) throws {
        switch self {
        case .int(let v):
            var container = encoder.singleValueContainer()
            try container.encode(v)
        case .double(let v):
            var container = encoder.singleValueContainer()
            try container.encode(v)
        case .string(let v):
            var container = encoder.singleValueContainer()
            try container.encode(v)
        case .data(let v):
            var container = encoder.singleValueContainer()
            try container.encode(v)
        case .date(let v):
            var container = encoder.singleValueContainer()
            try container.encode(v)
        case .null:
            var container = encoder.singleValueContainer()
            try container.encodeNil()
        }
    }
}

private struct PassthroughSerializer: Serializer {
    typealias SerializedType = Serializable
    
    func serialize(_ value: Serializable) throws -> Serializable {
        return value
    }
}

extension MySQLData {
    enum MySQLDataConversionError: Error {
        case compoundTypesNotSupported
    }
    
    public func toNative() throws -> DatabaseNativeType {
        let serialized = try PassthroughSerializer().encode(self)
        switch serialized {
        case .null:
            return .null
        case .int(let v):
            return .int(Int64(v))
        case .int8(let v):
            return .int(Int64(v))
        case .int16(let v):
            return .int(Int64(v))
        case .int32(let v):
            return .int(Int64(v))
        case .int64(let v):
            return .int(Int64(v))
        case .uint(let v):
            return .int(Int64(v))
        case .uint8(let v):
            return .int(Int64(v))
        case .uint16(let v):
            return .int(Int64(v))
        case .uint32(let v):
            return .int(Int64(v))
        case .uint64(let v):
            return .int(Int64(v))
        case .float(let v):
            return .double(Double(v))
        case .double(let v):
            return .double(Double(v))
        case .bool(let v):
            return .int(v ? 1 : 0)
        case .string(let v):
            return .string(v)
        case .data(let v):
            return .data(v)
        case .date(let v):
            return .date(v)
        case .array(_):
            throw MySQLDataConversionError.compoundTypesNotSupported
        case .dictionary(_):
            throw MySQLDataConversionError.compoundTypesNotSupported
        }
    }
}

private class MySQLConnectionWrapper: SwORM.Connection, BasicWorker {
    let mySQLDatabase: MySQLDatabase
    let wrappedConnection: MySQLConnection
    
    var inTransaction = false
    
    var database: SwORM.Database { return mySQLDatabase }
    
    func next() -> EventLoop { return wrappedConnection.next() }
    
    init(database: MySQLDatabase, connection: MySQLConnection) {
        self.mySQLDatabase = database
        self.wrappedConnection = connection
    }
    
    
    
    public func execute(_ query: String, parameters: [DatabaseType?]) -> EventLoopFuture<[Row]> {
        let futureResult: Future<[[MySQLColumn:MySQLData]]>
        
        if parameters.isEmpty && !query.trimmingCharacters(in: .whitespaces).hasPrefix("SELECT") {
            futureResult = wrappedConnection.simpleQuery(query)
        } else {
            futureResult = wrappedConnection.query(MySQLQuery._raw(query, parameters.map { $0?.asNative }))
        }
        
        return futureResult.map { (result: [[MySQLColumn:MySQLData]]) -> [Row]  in
            let rows = try result.map { row -> Row in
                var columns = [DatabaseNativeType]()
                var columnIndices = [String:Int]()
                for (key, value) in row {
                    columnIndices[key.name] = columns.endIndex
                    columns.append(try value.toNative())
                }
                return Row(columns: columns, columnIndices: columnIndices)
            }
            return rows
        }
    }
    
    enum ConnectionError: Error {
        case noRowsInserted
    }
    
    public func lastInsertedRow() -> EventLoopFuture<Int64> {
        return future(wrappedConnection.lastMetadata?.lastInsertID(as: Int64.self)).unwrap(or: ConnectionError.noRowsInserted)
    }
}
