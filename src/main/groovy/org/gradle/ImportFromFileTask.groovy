package org.gradle

import org.gradle.api.tasks.JavaExec
import org.gradle.api.tasks.TaskAction

import static org.gradle.utils.GradleUtils.readRequiredProperty

/**
 * Created with IntelliJ IDEA.
 * User: ddurkin
 * Date: 5/10/13
 * Time: 11:41 AM
 * To change this template use File | Settings | File Templates.
 */
class ImportFromFileTask extends JavaExec {

    String description = "import a gzipped file into a schema"

    @TaskAction
    def importDbFromFile() {
        final String alias = readRequiredProperty(project, 'dst')
        final String filename = readRequiredProperty(project, 'filename')

        main = 'com.github.Main'
        classpath = project.configurations.oracleSchemaCopyClasspath
        args = "import ${filename} ${alias} -properties gradle.properties".split().toList()
    }
}
