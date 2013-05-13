package com.github;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import groovy.lang.Binding;
import groovy.lang.Closure;
import groovy.lang.GString;
import groovy.lang.GroovyShell;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class Main {
    public static class Args {
        @Parameter
        public List<String> parameters = new ArrayList<String>();

        @Parameter(names = "-properties", description = "property file.  Defaults to ${user.home}/.oracle-schema-copy")
        public String propertyPath = System.getProperty("user.home") + "/.oracle-schema-copy";
    }

    static Properties readProperties(String path) {
        try {
            FileReader reader = new FileReader(path);
            Properties config = new Properties();
            config.load(reader);
            reader.close();
            return config;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }

    }

    public static class ScriptContext {
        Properties properties;

        public ScriptContext(Properties properties) {
            this.properties = properties;
        }

        Connection getConnection(String alias) {
            String url = properties.getProperty(alias + ".url");
            String username = properties.getProperty(alias + ".username");
            String password = properties.getProperty(alias + ".password");

            try {
                Connection connection = DriverManager.getConnection(url, username, password);
                connection.setAutoCommit(false);
                return connection;
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    public static void main(String[] argv) throws Exception {
        Args args = new Args();
        new JCommander(args, argv);

        Properties properties = readProperties(args.propertyPath);
        final ScriptContext context = new ScriptContext(properties);

        try {
            DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }

        // operations:
        //    disable/enable fks
        //    delete from a list of tables
        //    copy full database
        //    copy tree

        String cmd = args.parameters.get(0);
        if (cmd.equals("execute")) {
            String scriptName = args.parameters.get(1);
            executeScript(context, scriptName, args.parameters.subList(2, args.parameters.size()));
        } else if (cmd.equals("import")) {
            String filename = args.parameters.get(1);
            String alias = args.parameters.get(2);

            GZIPInputStream fis = new GZIPInputStream(new FileInputStream(filename));
            Connection connection = context.getConnection(alias);
            CopyUtils.importSchema(fis, connection);
        } else {
            throw new RuntimeException("Unknown command: " + cmd);
        }
    }

    private static String stringCast(Object o) {
        if (o instanceof String) {
            return (String) o;
        } else if (o instanceof GString) {
            return o.toString();
        } else {
            throw new RuntimeException("expected string but got " + o.getClass());
        }
    }

    private static void executeScript(final ScriptContext context, String scriptName, List<String> args) {
        Binding binding = new Binding();
        binding.setVariable("args", args);
        binding.setVariable("createConnection", new Closure<Connection>(null) {
            @Override
            public Connection call(Object... args) {
                return context.getConnection(stringCast(args[0]));
            }
        });
        binding.setVariable("createDbTarget", new Closure<Target>(null) {
            @Override
            public Target call(Object... args) {
                Connection connection = context.getConnection(stringCast(args[0]));
                return new ExecuteTarget(connection);
            }
        });
        binding.setVariable("createFileTarget", new Closure<Target>(null) {
            @Override
            public Target call(Object... args) {
                String filename = stringCast(args[0]);
                try {
                    return new OutputStreamTarget(new GZIPOutputStream(new FileOutputStream(filename)));
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            }
        });
        binding.setVariable("executeSql", new Closure<Void>(null) {
            @Override
            public Void call(Object... args) {
                Target target = (Target) args[0];
                String sql = stringCast(args[1]);
                target.apply(new ExecuteSqlList(Arrays.asList(sql)));
                return null;
            }
        });
        binding.setVariable("copyTree", new Closure<Void>(null) {
            @Override
            public Void call(Object... args) {
                Connection connection = (Connection) args[0];
                Target target = (Target) args[1];
                List<String> paths = (List) args[2];
                Collection rootIds = (Collection) args[3];
                String[] pathsv = paths.toArray(new String[0]);

                List<TableSelection> selections = CopyUtils.selectAlongPath(connection, pathsv, CopyUtils.getParentTableFromPath(stringCast(paths.get(0))), new ArrayList<Object>(rootIds));
                CopyUtils.copySelections(connection, selections, target);
                return null;
            }
        });
        binding.setVariable("deleteTree", new Closure<Void>(null) {
            @Override
            public Void call(Object... args) {
                Connection connection = (Connection) args[0];
                Target target = (Target) args[1];
                List<String> paths = (List) args[2];
                Collection rootIds = (Collection) args[3];
                String[] pathsv = paths.toArray(new String[0]);

                List<TableSelection> selections = CopyUtils.selectAlongPath(connection, pathsv, CopyUtils.getParentTableFromPath(stringCast(paths.get(0))), new ArrayList<Object>(rootIds));
                CopyUtils.deleteSelections(target, selections);
                return null;
            }
        });
        binding.setVariable("copy", new Closure<Void>(null) {
            @Override
            public Void call(Object... args) {
                Connection connection = (Connection) args[0];
                Target target = (Target) args[1];
                List<String> tables = (List) args[2];

                CopyUtils.addTableInsertOperations(target, connection, tables);
                return null;
            }
        });
        binding.setVariable("update", new Closure<Void>(null) {
            @Override
            public Void call(Object... args) {
                Connection connection = (Connection) args[0];
                Target target = (Target) args[1];
                List<String> tables = (List) args[2];

                CopyUtils.addTableUpdateOperations(target, connection, tables);
                return null;
            }
        });
//        binding.setVariable("export", new Closure<Void>(null) {
//            @Override
//            public Void call(Object... args) {
//                Connection connection = (Connection) args[0];
//                Target target = (Target) args[1];
//                String srcSchema = args[2].toString();
//                Collection dataTables = (Collection) args[3];
//
//                CopyUtils.exportAll(connection, target, srcSchema, dataTables);
//                return null;
//            }
//        });

        GroovyShell shell = new GroovyShell(binding);
        try {
            shell.evaluate(new File(scriptName));
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
