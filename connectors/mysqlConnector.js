var mysql = require('mysql');
var extend = require('xtend');
var graphlib = require('graphlib');

var types = {
    boolean: 'Boolean',
    number: 'Number',
    string: 'String',
    binary: 'Binary',
    datetime: 'DateTime'
};

function getTypeOfField(typeName)
{
    typeName = typeName.toLower();

    // Boolean types
    if(typeName.contains('bit')
    || typename.contains('bool')
    || typename == 'tinyint(1)')
    {
        return types.boolean;
    }

    // Numeric types
    if(typeName.contains('int') // bigint, integer, tinyint
    || typeName.contains('decimal')
    || typeName.contains('numeric')
    || typeName.contains('double')
    || typeName.contains('float')
    || typeName.contains('fixed')
    || typeName.contains('real'))
    {
        return types.number;
    }

    // String types
    if(typename.contains('char') // char, varchar
    || typename.contains('text'))
    {
        return types.string;
    }

    // Binary types
    if(typename.contains('binary') // binary varbinary
    || typename.contains('blob'))
    {
        return types.binary;
    }

    // Datetime types
    if(typename.contains('date') // date datetime
    || typename.contains('time') // time, timestamp
    || typename.contains('year'))
    {
        return types.datetime;
    }
}

function MysqlModel(pool)
{
    this.tables = new graphlib.Graph();
}

var defParams = {
    host: 'localhost',
    port: '3306',
    user: '',
    password: '',
    database: '',
    pool: true
};

function MysqlModeler(params)
{
    this.params = extend({}, defParams, params);
}

MysqlModeler.prototype.connect = function (callback)
{
    if(this.params.pool)
    {
        this.pool = mysql.createPool({
            host: params.host,
            port: params.port,
            user: params.user,
            password: params.password,
            database: params.database
        });

        this.pool.getConnection(function (err, connection)
        {
            this.context = connection;

            callback(err);
        });
    }
    else
    {
        this.context = mysql.createConnection({
            host: params.host,
            port: params.port,
            user: params.user,
            password: params.password,
            database: params.database
        });

        this.context.connect(function (err)
        {
            callback(err);
        });
    }
};

MysqlModeler.prototype.buildModel = function (callback)
{
    var model = new MysqlModel();

    // Figure out tables and make nodes for them
    this.context.query('show tables from ?', [this.params.database], function(err, rows, fields)
    {
        var tablesNodes = rows
                            .map(function (r) { return r[Object.keys(r)[0]]; })
                            .map(function (tn) { return new graphLib.Node(tn, [])});

        tableNodes.forEach(function (tn)
        {
            // Figure out columns in each row (both name and type of column)
            this.context.query('show columns from ?', [tn.id], function (err, rows)
            {
                var tableFields = rows
                                    .filter(function (r) { return r.Key !== 'PRI' && r.Key !== 'MUL'; })
                                    .map(function (r)
                                    {
                                        var fieldSpec = {};
                                        fieldSpec.name = r.Field;
                                        fieldSpec.type = getTypeOfField(r.Type);

                                        return fieldSpec;
                                    });

                tn.data.concat(tableFields);

                // TODO Figure out table relations
            });
        });
    });
};

module.exports = {
    establish: function (params, callback)
    {
        var mysqlModeler = new MysqlModeler(params);
        mysqlModler.connect(function (err)
        {
            callback(err, mysqlModler);
        });
    }
};
