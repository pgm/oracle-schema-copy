package org.gradle

import org.gradle.api.DefaultTask
import org.gradle.api.tasks.TaskAction

import java.sql.Connection
import java.util.zip.GZIPInputStream

import static com.github.CopyUtils.importSchema
import static com.github.utils.SqlUtils.getConnection
import static org.gradle.utils.GradleUtils.getConnectionInfo
import static org.gradle.utils.GradleUtils.readRequiredProperty

/**
 * Created with IntelliJ IDEA.
 * User: ddurkin
 * Date: 5/9/13
 * Time: 3:45 PM
 * Couldn't get the classpath issues resolved with this style of calling the import so create the ImportFromFileTask
 * which will run in a different java process
 */
class UpdateSchemaFromFileTask extends DefaultTask {
    String description = "import a schema from a serialized file in a regular gradle task, currently has issues with "

    @TaskAction
    def updateSchemaFromFile() {

        String alias = readRequiredProperty(project, 'dst')
        String filename = readRequiredProperty(project, 'filename')
        Map dstConnMap = getConnectionInfo(project, alias)
        println([alias, filename])
        GZIPInputStream fis = new GZIPInputStream(new FileInputStream(filename));
        Connection connection = getConnection(dstConnMap);
        importSchema(fis, connection);


    }
}
