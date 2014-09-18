/*jshint node:true */

var mysql = require('mysql');
var CONF = require('config');
var _ = require('underscore');
var util = require('util');
var stream = require('stream');

var tables = [];

// Extract data represented by a ? from a string
// e.x:
// var input = 'SomeData (5)';
// var five = input.extract('SomeData (?)');
// console.write(five); // prints '5' to the console
String.prototype.extract = function (str)
{
    var parts = str.split('?');

    var globalSearchData = [];

    var currentSearchStart = 0;
    var data = [];
    do
    {
        data = [];
        for(var i = 1; i < parts.length; i++)
        {
            var searchStart = '';
            for(var j = 0; j < data.length; j++)
            {
                searchStart += parts[j] + data[j];
            }
            searchStart += parts[data.length];
            var startIndex = this.indexOf(searchStart, currentSearchStart);

            if(startIndex >= 0)
            {
                startIndex += searchStart.length;
                var endIndex = this.indexOf(parts[i], startIndex);

                data.push(this.substring(startIndex, endIndex));
            }
            else
            {
                break;
            }
        }

        if(data.length > 0)
            globalSearchData.push(data);

        var finalSearchString = '';
        for(var k = 0; k < data.length; k++)
        {
            finalSearchString += parts[k] + data[k];
        }
        finalSearchString += parts[data.length];

        currentSearchStart = this.indexOf(finalSearchString, currentSearchStart) + 1;
    }
    while(data.length > 0);

    if(globalSearchData.length > 0)
    {
        return globalSearchData;
    }
};

String.prototype.insert = function (pos, str)
{
    return [this.slice(0, pos), str, this.slice(pos)].join('');
};

// Build a directed graph from the database capturing foreign key relationships
function modelDatabase(callback)
{
    MySqlConnector.context.query('show tables from ' + MySqlConnector.context.config.database, function(err, rows, fields)
    {
        var tableNames = _(rows).map(function (r) { return r[Object.keys(r)[0]]; });

        tables = _(tableNames).map(function (t)
        {
            return {
                name: t,
                relations: [],
                fields: []
            };
        });

        var numTables = tables.length;
        var doneTables = 0;

        _(tables).each(function (table)
       {
            MySqlConnector.context.query('show columns from ' + table.name, function (err, rows, fields)
            {
                table.fields = _.chain(rows)
                                .reject(function (r) { return r.Key == 'PRI' || r.Key == 'MUL'; })
                                .pluck('Field')
                                .value();

                MySqlConnector.context.query('show create table ' + table.name, function (err, rows, fields)
                {
                    if(err) throw err;

                    if(rows.length > 0 && rows[0]['Create Table'])
                    {
                        var ct = rows[0]['Create Table'];

                        var primaryKeySearch = 'PRIMARY KEY (`?`)';

                        var pk = ct.extract(primaryKeySearch);
                        if(pk)
                            table.id = pk[0][0];

                        var fkSearch = 'FOREIGN KEY (`?`) REFERENCES `?` (`?`)';
                        var fk = ct.extract(fkSearch);

                        if(fk)
                        {
                            _(fk).each(function (key)
                           {
                               var otherTable = _(tables).findWhere({name: key[1]});

                               var ourRelation = {
                                   table: otherTable,
                                   how: { ours: key[0], theirs: key[2] },
                                   what: 'ManyToOne'
                               };

                               var theirRelation = {
                                   table: table,
                                   how: { ours: key[2], theirs: key[0] },
                                   what: 'OneToMany'
                               };

                               otherTable.relations.push(theirRelation);
                               table.relations.push(ourRelation);
                           });
                        }
                    }
                    else
                    {
                        tables.splice(tables.indexOf(table), 1);
                    }

                    doneTables++;

                    if(doneTables == numTables && callback)
                        callback();
                });
            });
       });

    });
}

function dijkstra(targetTable, startingTable)
{
    var unvistednodes = {};
    _(tables).each(function (table)
    {
        unvistednodes[table.name] = { table: table, visited: false, distance: Infinity };
    });

    var targetNode = unvistednodes[targetTable.name];
    var startingNode = unvistednodes[startingTable.name];

    var currentNode = startingNode;
    currentNode.distance = 0;

    var previous = {};

    // Label distances for all nodes reachable from the source, term if we visit the target node
    while(currentNode && currentNode.distance != Infinity && !targetNode.visited)
    {
        var unvistedNeighbors = _.chain(currentNode.table.relations)
        .map(function (r) { return unvistednodes[r.table.name]; })
        .reject(function (n) { return !n; })
        .value();

        var dist = currentNode.distance + 1;

        _(unvistedNeighbors).each(function (n)
        {
            if(n.distance > dist)
            {
                n.distance = dist;
                previous[n.table.name] = currentNode;
            }
        });

        currentNode.visited = true;
        delete unvistednodes[currentNode.table.name];

        currentNode = _.chain(unvistednodes)
                      .min(function (n) { return n.distance; })
                      .value();
    }

    // Reconstruct path
    var path = [];

    if(targetNode.visited)
    {
        currentNode = targetNode;
        while(currentNode != startingNode)
        {
            path.unshift(currentNode.table);
            currentNode = previous[currentNode.table.name];
        }

        path.unshift(startingNode.table);
    }

    return path;
}

var MySqlConnector = {
    context: null,

    establish: function (callback)
    {
        MySqlConnector.pool = mysql.createPool({
            host: CONF.Database.host,
            user: CONF.Database.user,
            password: CONF.Database.password,
            database: CONF.Database.name
        });

        MySqlConnector.pool.getConnection(function (err, connection)
        {
            if(err) throw err;

            MySqlConnector.context = connection;

            modelDatabase(callback);
        });
    },

    Record: function (uid, raw, source)
    {
        var self = this;
        this.uid = uid;

        var table = source;

        Object.defineProperty(self, 'value_descriptions', {
            enumerable: false,
            writable: true,
            value: {}
        });

        _(raw).each(function (val, key)
        {
            var valData = {
                original: val,
                current: val,
                changed: false
            };

            self.value_descriptions[key] = valData;

            Object.defineProperty(self, key, {
                enumerable: true,
                get: function ()
                {
                    return self.value_descriptions[key].current;
                },
                set: function (nval)
                {
                    self.value_descriptions[key].current = nval;
                    self.value_descriptions[key].changed = true;
                }
            });
        });

        self.commit = function (callback)
        {
            if(self.uid)
            {
                var updateQuery = 'update ' + table.name + ' set ';

                _(table.fields).each(function (value)
                {
                    if (self.value_descriptions[value])
                        updateQuery += value + '=' + self.value_descriptions[value].current + ' ';
                });

                updateQuery += 'where ' + self.uid.name + '=' + self.uid.value;

                console.log(updateQuery);

                MySqlConnector.context.query(updateQuery, callback);
            }
            else
            {
                var insertQuery = 'insert into ' + table.name + ' (';
                var valuesQuery = 'values (';

                _(self.value_descriptions).each(function (value, key)
                {
                    insertQuery += key + ',';
                    valuesQuery += value.current + ',';
                });

                insertQuery = insertQuery.replace(/,$/g, ') ');
                valuesQuery = valuesQuery.replace(/,$/g, ')');

                console.log(insertQuery + valuesQuery);

                MySqlConnector.context.query(insertQuery + valuesQuery, callback);
            }
        };

        self.revert = function (params)
        {
            if(!params)
            {
                params = [];
                _(self.value_descriptions).each(function (v, k) { params.push(k); });
            }

            _(params).each(function (propName)
            {
                self.value_descriptions[propName].current = self.value_descriptions[propName].original;
                self.value_descriptions[propName].changed = false;
            });
        };
    },

    Collection: function (name, hint)
    {
        var self = this;

        var table = _(tables).findWhere({name: name});

        var selectClause = 'select distinct ' + name + '.*';
        var joinClause = '';

        _(table.relations).each(function (r)
        {
            if(r.what == 'ManyToOne' && r.table != table)
            {
                selectClause += ', ' + r.table.name + '.*';
                joinClause += ' left join ' + r.table.name
                            +' on ' + table.name + '.' + r.how.ours
                            + '=' + r.table.name + '.' + r.how.theirs;
            }
        });

        selectClause += ' from ' + name;

        var whereClause = '';
        var orderbyClause = '';
        var orderbyDirectionClause = '';
        var reverseClause = false;
        var setClause= '';
        var gbClause = '';

        var fields = table.fields;
        var pk = table.id;

        function aggregateWhereClause(op, conj, params)
        {
            var whereParts = _(params).map(function (value, key)
            {
                if(_(fields).contains(key))
                {
                    return  key + op + value;
                }
                else if(pk == key)
                {
                    return pk + op + value;
                }
                else if(_(table.relations).any(function (r) { return r.table.name == key; }))
                {
                    var relation = _(table.relations).find(function (r) { return r.table.name == key });
                    return table.name + '.' + relation.how.ours + op + value.uid.value;
                }
                else
                {
                    var targetTable = _(tables).findWhere({name: key});
                    var path = dijkstra(targetTable, table);

                    if(path.length > 0)
                    {
                        for(var i = 0; i < path.length - 2; i++)
                        {
                            var jc = '';

                            var tableLeft = path[i];
                            var tableRight = path[i+1];

                            var relation = _(tableLeft.relations).findWhere({table: tableRight});

                            jc += ' inner join ' + tableRight.name
                                + ' on ' + tableLeft.name + '.' + relation.how.ours
                                + '=' + tableRight.name + '.' + relation.how.theirs;

                            joinClause += jc;
                        }

                        return path[path.length - 2].name + '.' + value.uid.name + op + value.uid.value;
                    }
                }
            });

            var where = whereParts.join(' ' + conj + ' ');

            if(where !== '')
            {
                if(whereClause !== '')
                    whereClause += ' ' + conj + ' ';

                whereClause += where;
            }

            return self;
        }

        self.with = _(aggregateWhereClause).partial('=', 'and');

        self.without = _(aggregateWhereClause).partial('<>', 'and');

        self.any = _(aggregateWhereClause).partial('=', 'or');

        self.orderby = function (params)
        {
            if(params && params.length > 0)
            {
                if(orderbyClause !== '')
                    orderbyClause += ',';

                orderbyClause += params.join();
            }

            return self;
        };

        self.count = function ()
        {

        };

        self.rand = function (n)
        {
            if(!n)
                n = 1;

            orderbyClause = 'rand() limit ' + n;

            return self;
        };

        self.reverse = function ()
        {
            reverseClause = !reverseClause;
            return self;
        };

        self.union = function (other)
        {
            setClause += ' union (' + other.query() + ')';
            return self;
        };

        self.intersect = function (other, on)
        {
            var otherQ = other.query();

            var selectEnd = otherQ.indexOf('from');

            if(!on)
                on = pk;

            var queryNoSelect = otherQ.substr(selectEnd);
            var transformedQuery = 'select distinct ' + table.name + '.' + on + ' ' + queryNoSelect;

            if(whereClause !== '')
                whereClause += ' and ';

            whereClause += table.name + '.' + on + ' in (' + transformedQuery + ')';

            return self;
        };

        self.difference = function (other)
        {
            var otherQ = other.query();

            var selectEnd = otherQ.indexOf('from');

            var queryNoSelect = otherQ.substr(selectEnd);
            var transformedQuery = 'select distinct ' + table.name + '.' + pk + ' ' + queryNoSelect;

            if(whereClause !== '')
                whereClause += ' and ';

            whereClause += table.name + '.' + pk + ' not in (' + transformedQuery + ')';

            return self;
        };

        self.project = function (other)
        {
            var projectedTable = _(tables).findWhere({name: other});

            var relation = _(table.relations).findWhere({table: projectedTable});

            selectClause = 'select distinct ' + projectedTable.name + '.* ';
            joinClause = ' left join ' + projectedTable.name
                        + ' on ' + table.name + '.' + relation.how.ours
                        + '=' + relation.table.name + '.' + relation.how.theirs;

            _(projectedTable.relations).each(function (r)
            {
                if(r.what == 'ManyToOne')
                {
                    selectClause += ', "' + r.table.name + '", ' + r.table.name + '.*';
                    joinClause += ' left join ' + r.table.name
                                +' on ' + projectedTable.name + '.' + r.how.ours
                                + '=' + r.table.name + '.' + r.how.theirs;
                }
            });

            selectClause += ' from ' + table.name;

            table = projectedTable;
            pk = table.id;

            return self;
        };

        self.query = function ()
        {
            var query = selectClause;

            if(joinClause !== '')
            {
                query += joinClause + ' ';
            }

            if(hint)
            {
                if(whereClause)
                    whereClause = '(' + whereClause + ') and ' + hint;
                else
                    whereClause = hint;
            }

            if(whereClause !== '')
                query += ' where ' + whereClause;

            if(orderbyClause !== '')
            {
                query += ' order by ' + orderbyClause;

                if(reverseClause)
                {
                    query += ' desc';
                }
            }

            if(gbClause !== '')
            {
                query += ' group by ' + gbClause;
            }

            if(setClause !== '')
            {
                query = '(' + query + ')' + setClause;
            }

            return query;
        };

        self.table = function ()
        {
            return table;
        };

        self.source = function ()
        {
            return table;
        };

        self.exec = function (callback)
        {
            var query = self.query();

            console.log(query);

            MySqlConnector.context.query({sql: query, nestTables: true}, function (erro, rows, fields)
            {
                if(erro) throw erro;

                var records = _(rows).map(_(MySqlConnector.MakeRecord).partial(table));

                callback(records);
            });
        };

        self.stream = function (context)
        {
            var query = self.query();

            console.log(query);

            var streamObj = context.query({sql: query, nestTables: true}).stream();
            var records = new MySqlConnector.RecordStream({table: table});

            streamObj.pipe(records);

            return records;
        };
    },

    RecordStream: function(options)
    {
        stream.Transform.call(this, {objectMode: true});

        this.options = options;
        this.table = options.table;
    },

    MakeRecord: function(table, row)
    {
        var subrecords = {};
        _(table.relations).each(function (r)
        {
            if(r.what == 'ManyToOne')
            {
                var raw = row[r.table.name];

                var subuid = { name: r.table.id, value: row[r.table.name][r.table.id] };
                delete row[r.table.name][r.table.id];

                subrecords[r.table.name] = new MySqlConnector.Record(subuid, raw, r.table);
            }
        });

        var uid = { name: table.id, value: row[table.name][table.id] };
        delete row[table.name][table.id];
        var record = new MySqlConnector.Record(uid, row[table.name], table);

        _(subrecords).each(function (sr, key)
        {
            record[key] = sr;
        });

        return record;
    }
};

util.inherits(MySqlConnector.RecordStream, stream.Transform);

MySqlConnector.RecordStream.prototype._transform = function (row, encoding, callback)
{
    var record = MySqlConnector.MakeRecord(this.table, row);
    this.push(record);
    callback();
};

module.exports = MySqlConnector;
