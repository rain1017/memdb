// Copyright 2015 rain1017.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

'use strict';

module.exports = function(grunt) {
    // Unified Watch Object
    var watchFiles = {
        libJS: ['gruntfile.js', 'index.js', 'app/**/*.js', 'bin/**/*.js', 'lib/**/*.js'],
        testJS: ['test/app/*.js', 'test/bin/*.js', 'test/lib/*.js', 'test/perf.js'],
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
            // coverage: {
            //     src: watchFiles.testJS,
            //     options : {
            //         reporter: 'html-cov',
            //         quiet: true,
            //         captureFile: 'coverage.html'
            //     }
            // }
        },
        clean: {
            'coverage.html' : {
                src: ['coverage.html']
            }
        },
        nodemon: {
            dev: {
                script: 'bin/memdbd',
                options: {
                    args : ['--conf=./test/memdb.conf.js', '--shard=s1'],
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
