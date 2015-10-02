var gulp = require('gulp'),
    jade = require('gulp-jade'),
    jshint = require('gulp-jshint'),
    watch = require('gulp-watch'),
    sass = require('gulp-sass');

/** Jade **/
gulp.task('jade', function () {
    return gulp.src('./src/**/*.jade')
        .pipe(jade())
        .pipe(gulp.dest('./app'));
});

/** JSHint **/
gulp.task('jshint', function () {
    return gulp.src('./src/**/*.js')
        .pipe(jshint())
        .pipe(jshint.reporter('default'))
        .pipe(gulp.dest('./app'));
});

gulp.task('sass', function () {
  gulp.src('./src/sass/**/*.scss')
    .pipe(sass().on('error', sass.logError))
    .pipe(gulp.dest('./app/css'));
});

/** Watch **/
gulp.task('watch', function () {
    gulp.watch('./src/**/*.js', ['jshint']);
    gulp.watch('./src/**/*.jade', ['jade']);
    gulp.watch('./src/**/*.scss', ['sass']);
});

/** Default task **/
gulp.task('default', ['jshint', 'jade', 'sass', 'watch']);