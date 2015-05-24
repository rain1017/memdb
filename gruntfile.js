'use strict';

module.exports = function(grunt) {
    // Unified Watch Object
    var watchFiles = {
        libJS: ['gruntfile.js', 'index.js', 'app/**/*.js', 'lib/**/*.js'],
        testJS: ['test/**/*.js'],
    };

    // Project Configuration
    grunt.initConfig({
        pkg: grunt.file.readJSON('package.json'),
        env: {
            test: {
                NODE_ENV: 'test'
            }
        },
        watch: {
            libJS: {
                files: watchFiles.libJS.concat(watchFiles.testJS),
                tasks: ['jshint'],
                options: {
                    livereload: true
                }
            }
        },
        jshint: {
            all: {
                src: watchFiles.libJS.concat(watchFiles.testJS),
                options: {
                    jshintrc: true
                }
            }
        },
        mochaTest: {
            test : {
                src: watchFiles.testJS,
                options : {
                    reporter: 'spec',
                    timeout: 10000,
                    require: 'test/blanket'
                }
            },
        },
        clean: {
            'coverage.html' : {
                src: ['coverage.html']
            }
        },
        nodemon: {
            dev: {
                script: 'app/server.js',
                options: {
                    args : ['--conf=./test/memdb.json', '--shard=s1'],
                    nodeArgs: ['--debug'],
                    ext: 'js,html',
                    watch: watchFiles.serverJS
                }
            }
        },
        'node-inspector': {
            custom: {
                options: {
                    'web-port': 8088,
                    'web-host': 'localhost',
                    'debug-port': 5858,
                    'save-live-edit': true,
                    'no-preload': true,
                    'stack-trace-limit': 50,
                    'hidden': []
                }
            }
        },
        concurrent: {
            default: ['nodemon', 'watch'],
            debug: ['nodemon', 'watch', 'node-inspector'],
            options: {
                logConcurrentOutput: true,
                limit: 10
            }
        },
    });

    // Load NPM tasks
    require('load-grunt-tasks')(grunt);

    // Making grunt default to force in order not to break the project.
    grunt.option('force', true);

    // Lint task(s).
    grunt.registerTask('lint', ['jshint']);

    // Test task.
    grunt.registerTask('test', ['clean', 'lint', 'env:test', 'mochaTest']);

    // Run server
    grunt.registerTask('default', ['clean', 'lint', 'concurrent:default']);

    // Debug server
    grunt.registerTask('debug', ['clean', 'lint', 'concurrent:debug']);
};
