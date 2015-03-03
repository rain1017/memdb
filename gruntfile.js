'use strict';

module.exports = function(grunt) {
	// Unified Watch Object
	var watchFiles = {
		libJS: ['gruntfile.js', 'index.js', 'lib/**/*.js'],
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
			coverage: {
				src: watchFiles.testJS,
				options : {
					reporter: 'html-cov',
					quiet: true,
					captureFile: 'coverage.html'
				}
			}
		},
		clean: {
			'coverage.html' : {
				src: ['coverage.html']
			}
		},
		concurrent: {
			dev: {
				tasks: ['nodemon', 'node-inspector'],
				options: {
					logConcurrentOutput: true
				}
			},
			debug: {
				tasks: ['nodemon:dev', 'node-inspector'],
				options: {
					logConcurrentOutput: true
				}
			},
			'debug-test': {
				tasks: ['nodemon:debug-test', 'node-inspector', 'watch:testsJS'],
				options: {
					logConcurrentOutput: true
				}
			}
		},
		nodemon: {
			'debug-test': {
				script: '/usr/local/lib/node_modules/mocha/bin/_mocha',
				options: {
					nodeArgs: ['--debug=31001', '--debug-brk'],
					args: ['tests/compiler_test.js'],
					watch: ['tests'],
					ext: 'js',
					callback: function (nodemon) {
						nodemon.on('log', function (event) {
							console.log(event.colour);
						});
						// opens browser on initial server start
						nodemon.on('config:update', function () {
							// Delay before server listens on port
							setTimeout(function() {
								require('open')('http://localhost:31010/debug?port=31001');
							}, 1000);
						});
					}
				}
			},
			dev: {
				script: 'msample.js',
				options: {
					nodeArgs: ['--debug=31001', '--debug-brk'],
					env: {
						PORT: '31000'
					},
					ignore: [
						'public/**'
					],
					watch: ['node_modules/**/*.js', 'views/**/*.js', 'services/**/*.js', '*.js', 'models/**/*.js'],
					ext: 'js',
					callback: function (nodemon) {
						nodemon.on('log', function (event) {
							console.log(event.colour);
						});
						// opens browser on initial server start
						nodemon.on('config:update', function () {
							// Delay before server listens on port
							setTimeout(function() {
								require('open')('http://localhost:31010/debug?port=31001');
							}, 1000);
						});
						// refreshes browser when server reboots
						nodemon.on('restart', function () {
							// Delay before server listens on port
							setTimeout(function() {
								require('fs').writeFileSync('.rebooted', 'rebooted');
							}, 1000);
						});
					}
				}
			}
		},
		'node-inspector': {
			custom: {
				options: {
					'web-port': 31010,
					'web-host': 'localhost',
					'debug-port': 31001,
					'save-live-edit': true,
					'no-preload': false,
					'stack-trace-limit': 100
				}
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

	// Default task(s).
	grunt.registerTask('default', ['test']);
};
