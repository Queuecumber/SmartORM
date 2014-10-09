var Promise = require('promise');

module.exports = {
    // Available Connectors
    connectors: {
        mysql: 'connectors/mysqlConnector'
    },

    model: function (connector, params, callback)
    {
        var connector = require(connector);

        var p = new Promise(function (fulfill, reject)
        {
            connector.establish(params, function (err, modeler)
            {
                if(err)
                {
                    reject(err);
                }
                else
                {
                    modeler.buildModel(function (err, model)
                    {
                        if(err)
                        {
                            reject(err);
                        }
                        else
                        {
                            fulfill(model)
                        }
                    });
                }
            });
        });

        return p.nodeify(callback);
    }
};
