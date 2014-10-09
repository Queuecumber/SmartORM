var graphlib = require('graphlib');
var Promise = require('promise');

function Collection(node)
{
    this.select = '*';
    this.from = node.id;
    this.where = null;
    this.groupBy = null;
    this.orderBy = null;
}

Collection.prototype.with = function (params)
{

};

function MysqlModel(pool, nodes)
{
    this.context = pool;
    this.tables = new graphlib.Graph();
    this.tables.nodes = nodes;

    nodes.forEach(function (n)
    {
        this[n.id] = new Collection(n);
    }, this);
}
