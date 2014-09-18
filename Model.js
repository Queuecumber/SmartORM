/*jshint node:true */

// Implement "connectors"
// Used to build a "query" using functions:
// with, without, any, orderby, reverse, (groupby => how should this look?), count, min, max, minmax, accumulate, all, some, (map => how should this look?), rand
// execute query with exec, stream results with stream

// with => and
// without => nand (and row.a != param.a ...)
// any => or
// accumulate => sum
// all, some => boolean return only (e.g. true or false if (all => every, some => at least one) value returned by the other operations passes their test)
// rand => return n random elements
// union, intersection, difference => set operations
// project => projects results onto a related table, similar to a join

// Provide different implementations for different databases, allows switching backend dbms without changing RPC calls, but RPC calls are still useful rather
// than just forwarding calls to the db level
// Database level stuff should be abstracted: no select, no primary/foreign keys, no joins, nothing that depends on a particular dbms

// Each step (e.g. function call) gives you a new, updated query that can be executed with the exec call. Results are created using
// Object.addProperty (or w/e its called) to allow setters to be serialized into database operations which can be serialzied back
// to the target database with a commit() function, while getters provide 'normal' js data

var CONF = require('config');
var connector = require('./' + CONF.Database.connector);

var Model = {
    
    init: function (callback)
    {
        var initializedFields = 0;
        var fieldCount = 4;
        
        connector.establish(callback);
    },
  
    detections: function () { return new connector.Collection('concept_instances'); },

    concepts: function() { return new connector.Collection('concepts', 'is_event=false'); },

    conceptTypes: function() { return new connector.Collection('concept_type'); },

    events: function() { return new connector.Collection('concepts', 'is_event=true'); },

    datasets: function() { return new connector.Collection('datasets'); },

    datasetVideos: function() { return new connector.Collection('dataset_videos'); },

    fileDirectories: function() { return new connector.Collection('file_directory'); },

    tasks: function() { return new connector.Collection('tasks'); },

    backendTasks: function() { return new connector.Collection('backend_tasks'); },

    taskEvents: function() { return new connector.Collection('task_events'); },

    videos: function() { return new connector.Collection('videos'); },
   
    conceptInstanceStatistics: function() { return new connector.Collection('concept_instance_statistics'); },

    users: function() { return new connector.Collection('users'); },

    connector: connector
};

module.exports = Model;
