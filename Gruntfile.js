module.exports = function(grunt) {

	grunt.initConfig({
		pkg: grunt.file.readJSON('package.json'),
		coffeelint: {
			options: {
				'no_tabs': {
					'level': 'ignore'
				},
				'indentation': {
					'level': 'ignore'
				},
				'max_line_length': {
					'level': 'ignore'
				}
			},
			app: ['*.coffee']
		},
		coffee: {
			compile_all : {
				expand: true,
				flatten: false,
				src: ['./*.coffee'],
				dest: './',
				ext: '.js'
			}
		},
		clean: {
			allJavascriptFiles: [
				'twitterbot.js'
			]
		},
		concurrent: {
			development: {
				tasks: ['coffeelint','coffee', 'watch', 'nodemon'],
				options: {
					logConcurrentOutput: true
				}
			}
		},
		nodemon: {
			development: {
				options: {
					watchedFolders: ['.'],
					delayTime: 5
				}
			}
		},
		watch: {
			files: ['*.coffee'],
			tasks: ['coffeelint',  'coffee']
		}
	});

	grunt.loadNpmTasks('grunt-coffeelint');
	grunt.loadNpmTasks('grunt-contrib-watch');
	grunt.loadNpmTasks('grunt-contrib-coffee');
	grunt.loadNpmTasks('grunt-contrib-clean');
	grunt.loadNpmTasks('grunt-nodemon');
	grunt.loadNpmTasks('grunt-concurrent');

	grunt.registerTask('development', ['coffee']);
	grunt.registerTask('default', ['concurrent:development']);
};