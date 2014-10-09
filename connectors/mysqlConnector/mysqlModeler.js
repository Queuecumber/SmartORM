var mysql = require('mysql');
var extend = require('xtend');
var graphlib = require('graphlib');
var Promise = require('promise');
var MysqlModel = require('./mysqlModel');

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

MysqlModeler.prototype.getTypeOfField = function (typeName)
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
};

MysqlModeler.prototype.connect = function (callback)
{
    var pquery = function (stmnt, params)
    {
        return new Promise(function (fulfill, reject)
        {
            var qf = null;
            if(params)
                qf = this.query.bind(this, stmnt, params);
            else
                qf = this.query.bind(this, stmnt);


            qf(function (err, rows, fields)
            {
                if(err)
                {
                    reject(err);
                }
                else
                {
                    fulfill(rows, fields);
                }
            });
        }.bind(this));
    };

    if(this.params.pool)
    {
        this.pool = mysql.createPool({
            host: params.host,
            port: params.port,
            user: params.user,
            password: params.password,
            database: params.schema
        });

        this.pool.getConnection(function (err, connection)
        {
            this.context = connection;
            this.context.pquery = pquery.bind(this.context);

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
            database: params.schema
        });

        this.context.connect(function (err)
        {
            this.context.pquery = pquery.bind(this.context);

            callback(err);
        });
    }
};

MysqlModeler.prototype.modelTablesAsNodes = function ()
{
    // Figure out tables and make nodes for them
    return this.context.pquery('show tables from ?', [this.params.database])
        .then(function (rows, fields)
        {
            return rows
                .map(function (r) { return r[fields[0].name]; })
                .map(function (tn) { return new graphLib.Node(tn)});
        }.bind(this));
};

MysqlModeler.prototype.modelColumnsAsFields = function (tn)
{
    // Figure out columns in each row (both name and type of column)
    return this.context.pquery('show columns from ?', [tn.id])
        .then(function (rows)
        {
            tn.fields = rows.map(function (r)
            {
                var fieldSpec = {
                    name: r.Field,
                    type: this.getTypeOfField(r.Type)
                };

                return fieldSpec;
            }, this);

            return tn;
        }.bind(this));
};

MysqlModeler.prototype.modelRelationsAsEdges = function (tn)
{
    // Figure out table relations
    return this.context.pquery('show create table ?', [tn.id])
        .then(function (rows)
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

            return tn;
        });
};

MysqlModeler.prototype.buildModel = function (callback)
{
    return this.modelTablesAsNodes()
        .then(function (tableNodes)
        {
            return Promise.all(tableNodes.map(this.modelColumnsAsFields.bind(this)));
        }.bind(this))
        .then(function (tableNodes)
        {
            return Promise.all(tableNodes.map(this.modelRelationsAsEdges.bind(this)));
        }.bind(this))
        .then(function (tableNodes)
        {
            var model = new MysqlModel(this.context, tableNodes);
            return model;
        }.bind(this))
        .nodeify(callback);
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
