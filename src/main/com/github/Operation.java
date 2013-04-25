/**
 *
 */
package com.github;

import java.io.Serializable;
import java.sql.Connection;

interface Operation extends Serializable {
    public void execute(Connection connection);
}