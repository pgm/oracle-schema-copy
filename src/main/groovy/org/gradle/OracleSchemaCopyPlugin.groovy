package org.gradle

import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.tasks.JavaExec

import static org.gradle.utils.GradleUtils.ensureRequiredProperties
import static org.gradle.utils.GradleUtils.prodCheck

/**
 * Created with IntelliJ IDEA.
 * User: ddurkin
 * Date: 5/9/13
 * Time: 11:16 AM
 *
 */
class OracleSchemaCopyPlugin implements Plugin<Project> {

    final static String USAGE_MESSAGE = """
oracleSchemaCopy provides some utilities for importing and exporting ddls and data
example commands
gradle exportToFile -Psrc=<someAlias>
    <someAlias> should match a prefix in the gradle.properties file for a set of db connection info
gradle importFromFile -Pdst=<someAlias> -Pfilename=<someDumpFile.gz
    <someAlias> should match a prefix in the gradle.properties file for a set of db connection info
    <someDumpFile> should be the path to a gzipped file that was created via exportToFile

These tasks expect a gradle.properties file with values of like:
devenv.url=jdbc:oracle:thin:@localhost:1522:AL32UTF8
devenv.username=devenv
devenv.password=devenv

excluded.dataTables=AUDIT_COLUMN_LOG,AUDIT_ROW_LOG,DATABASECHANGELOGLOCK
excluded.schemaTables=DATABASECHANGELOGLOCK,DATABASECHANGELOG

In the above example, devenv should match the <someAlias> params above in order to identify groups of db connection info""".trim()


    @Override
    void apply(Project project) {

        project.task('oracleSchemaCopyUsage', description: 'a description of usage') << {
            println(USAGE_MESSAGE)
        }
        project.task('exportToFile', type: ExportToFileTask)

        project.task('importFromFile', type: JavaExec, description: 'import a snapshot into a schema from a gzipped file')

        project.tasks.importFromFile.doFirst { task ->
            ensureRequiredProperties(project, ['filename', 'dst'])
            prodCheck(project)
            task.main = 'com.github.Main'
            task.classpath = project.configurations.oracleSchemaCopyClasspath
            task.args = "import ${project['filename']} ${project['dst']} -properties gradle.properties".split().toList()
        }
    }
}