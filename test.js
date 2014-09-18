var smart = require('./smart');

smart.model(smart.connectors.mySql, {
    schema: 'forum',
    user: 'mehrlich',
    password: ''
}).then(function (model)
{
    // Standard async
    model.user.with({name: 'chinese john'}).exec(function (err, user)
    {
        if(err)
            console.log(err);
        else
            console.log(user);

        console.log('Done!');
    });

    // Promise
    model.user.with({name: 'chinese john'})
        .then(function (user)
        {
            console.log(user);
        })
        .catch(function (err)
        {
            console.log(err);
        })
        .then(function ()
        {
            console.log('Done!');
        });

    // Stream
    var strm = model.user.with({name: 'chinese john'}).stream();

    strm.on('data', function (user)
    {
        console.log(user);
    });

    strm.on('error', function (err)
    {
        console.log(err);
    });

    str.on('end', function ()
    {
        console.log('Done!');
    });

    // strm.pipe(socketIO);
})
.catch(function (err)
{
    console.log(err);
});
