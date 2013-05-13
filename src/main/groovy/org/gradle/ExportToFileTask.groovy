package org.gradle

import org.gradle.api.DefaultTask
import org.gradle.api.tasks.TaskAction

import java.sql.Connection

import static com.github.CopyUtils.exportAll
import static com.github.CopyUtils.getTableNames
import static com.github.FileUtils.createFileTarget
import static com.github.utils.SqlUtils.getConnection
import static org.gradle.utils.GradleUtils.*

/**
 * Created with IntelliJ IDEA.
 * User: ddurkin
 * Date: 5/9/13
 * Time: 1:27 PM
 * To change this template use File | Settings | File Templates.
 */
class ExportToFileTask extends DefaultTask {
    String description = "will export a schema to gzipped file"

    @TaskAction
    def exportSchemaToFile() {
        ensureRequiredProperties(project, ['src'])
        final String alias = project['src']

        Map srcConnMap = getConnectionInfo(project, alias)
        Connection srcConn = getConnection(srcConnMap)
        List<String> tables = getTableNames(srcConn)
        tables.sort(true)
        println(tables)

        Set<String> dataTables = filterExcludedTables(tables, project["excluded.dataTables"].split(',') as List)

        println("dataTables has the follwoing tables removed : ${tables - dataTables}")
        Set<String> schemaTables = filterExcludedTables(tables, project["excluded.schemaTables"].split(',') as List)

        println("schemaTables has the follwoing tables removed : ${tables - schemaTables}")
        def dst = createFileTarget("${alias}-dump.ser.gz")
        exportAll(srcConn, dst, alias.toUpperCase(), schemaTables, dataTables)
        dst.close()
    }
}
