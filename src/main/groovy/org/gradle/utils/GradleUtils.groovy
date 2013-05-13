package org.gradle.utils

import org.gradle.api.GradleException
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

    static void ensureRequiredProperties(Project project, List<String> propertyNames) {
        List errors = validateRequiredProperties(project, propertyNames)
        if (errors) {
            throw new GradleException(errors.join('\n'))
        }
    }

    static List<String> validateRequiredProperties(Project project, List<String> propertyNames) {
        List<String> errors = []
        for (String propertyName in propertyNames) {
            if (!project.hasProperty(propertyName)) {
                errors.add("Missing property '${propertyName}', please specify -P${propertyName}=someValue")
            }
        }
        errors
    }

    static void prodCheck(Project project) {
        if (project.hasProperty('dst') ) {
            String jdbcUrl = getConnectionInfo(project, project['dst']).url?.toLowerCase()
            if (jdbcUrl && jdbcUrl.contains('prod') && !project.hasProperty('dstProduction')){
              String msg = ["In order to have a dst that contains prod in the jdbc url, you have to specify -PdstProduction.",
                                "Be sure this is what you want to do!"].join('\n')
                throw new GradleException(msg)
            }
        }
    }
}