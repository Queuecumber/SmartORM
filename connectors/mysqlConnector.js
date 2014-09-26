var mysql = require('mysql');
var extend = require('xtend');
var graphlib = require('graphlib');

var tableRelationships = {
    manyToOne: 'ManyToOne',
    oneToMany: 'OneToMany',
    manyToMany: 'ManyToMany'
};

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
                            .map(function (tn) { return new graphLib.Node(tn)});

        model.tables.nodes = tableNodes;

        // Build a graph of the tables and their relationships
        model.tables.nodes.forEach(function (tn)
        {
            // Figure out columns in each row (both name and type of column)
            this.context.query('show columns from ?', [tn.id], function (err, rows)
            {
                var tableFields = rows
                                    .map(function (r)
                                    {
                                        var fieldSpec = {
                                            name: r.Field,
                                            type: getTypeOfField(r.Type)
                                        };

                                        return fieldSpec;
                                    });

                tn.fields = tableFields;

                // Figure out table relations
                this.context.query('show create table ?', [tn.id], function (err, rows)
                {
                    // Figure out primary keys
                    // TODO support multiple column keys
                    var createText = rows['Create Table'];

                    var primaryKeyMatcher = /PRIMARY KEY \(`(.*)`\)/g;
                    var keys = [];

                    while(var result = primaryKeyMatcher.exec(createText))
                        keys.push(result[1]);

                    tn.keys = keys;

                    // Figure out foreign keys and added appropriate edges
                    var foreignKeyMatcher = /CONSTRAINT `.*` FOREIGN KEY \(`(.*)`\) REFERENCES `(.*)` \(`(.*)`\)/g;

                    while(var result = foreignKeyMatch.exec(createText))
                    {
                        var otherTable = model.tables.node(result[2]);

                        var ourRelation = {
                            table: otherTable,
                            how: {
                                ours: tn.fields.find(function (f) { return f.name === result[1]; }),
                                theirs: otherTable.fields.find(function (f) { return f.name === result[3] })
                            },
                            direction: tableRelationships.manyToOne
                        };

                        var theirRelation = {
                            table: tn,
                            how: {
                                ours: ourRelation.how.theirs,
                                theirs: ourRelation.how.ours
                            },
                            direction: tableRelationships.oneToMany
                        };

                        var ourEdge = new graphLib.Edge(otherTable, 1);
                        ourEdge.relation = ourRelation;

                        var theirEdge = new graphLib.Edge(tn, 1);
                        theirEdge.relation = theirRelation;

                        tn.edges.push(ourEdge);
                        otherTable.edges.push(theirEdge);
                    }
                });
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
