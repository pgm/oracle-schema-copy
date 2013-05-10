package org.gradle.utils

import org.gradle.api.Project

/**
 * Created with IntelliJ IDEA.
 * User: ddurkin
 * Date: 5/9/13
 * Time: 1:43 PM
 * To change this template use File | Settings | File Templates.
 */
class GradleUtils {
    static Map getConnectionInfo(Project project, String alias) {
        [url: project["${alias}.url"], username: project["${alias}.username"], password: project["${alias}.password"]]
    }

    static Set<String> filterExcludedTables(List<String> allTables, List<String> excludedTables) {
        Set<String> filteredTables = [] as Set
        filteredTables.addAll(allTables)
        filteredTables.removeAll(excludedTables)
        filteredTables
    }

    static String readRequiredProperty(Project project, String propertyName) {
        if (!project.hasProperty(propertyName)) {
            project.ant.fail("${propertyName} project property required be sure to add -P${propertyName}=foo")
        }
    }
}