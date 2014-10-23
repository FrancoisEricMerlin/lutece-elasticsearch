/*
 * Copyright (c) 2002-2014, Mairie de Paris
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 *  1. Redistributions of source code must retain the above copyright notice
 *     and the following disclaimer.
 *
 *  2. Redistributions in binary form must reproduce the above copyright notice
 *     and the following disclaimer in the documentation and/or other materials
 *     provided with the distribution.
 *
 *  3. Neither the name of 'Mairie de Paris' nor 'Lutece' nor the names of its
 *     contributors may be used to endorse or promote products derived from
 *     this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *
 * License 1.0
 */
package fr.paris.lutece.plugins.elasticsearch.service;

import fr.paris.lutece.portal.service.util.AppLogService;
import fr.paris.lutece.portal.service.util.AppPathService;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.Node;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

public class ElasticsearchServer
{
    private static final String KEY_CONFIG = "path.conf";
    private static final String PATH_CONFIG = "/WEB-INF/plugins/elasticsearch/config";
    private static final String KEY_DATA = "path.data";
    private static final String PATH_DATA = "/WEB-INF/plugins/elasticsearch/data";
    private static final String KEY_LOGS = "path.logs";
    private static final String PATH_LOGS = "/WEB-INF/logs";
    private static final String KEY_PLUGINS = "path.plugins";
    private static final String PATH_PLUGINS = "/WEB-INF/plugins/elasticsearch/plugins";
    
    protected Map<String, String> _configuration;
    private Node _server;

    /**
     * Constructor
     * @param configuration 
     */
    public ElasticsearchServer(Map<String, String> configuration)
    {
        _configuration = configuration;
        _configuration.put( KEY_CONFIG, AppPathService.getAbsolutePathFromRelativePath(PATH_CONFIG));
        _configuration.put( KEY_DATA, AppPathService.getAbsolutePathFromRelativePath(PATH_DATA));
        _configuration.put( KEY_LOGS, AppPathService.getAbsolutePathFromRelativePath(PATH_LOGS));
        _configuration.put( KEY_PLUGINS, AppPathService.getAbsolutePathFromRelativePath(PATH_PLUGINS));
    }

    /**
     * Start the server
     */
    public void start()
    {
        AppLogService.info("Starting the Elastic Search server node");

        ImmutableSettings.Builder builder
                = ImmutableSettings.settingsBuilder().put(_configuration);
        _server = nodeBuilder().settings(builder).build();

        if ("true".equalsIgnoreCase(System.getProperty("es.max.files")))
        {
            String workPath = _server.settings().get("path.work");
            int maxOpen = maxOpenFiles(new File(workPath));
            AppLogService.info("The maximum number of open files for user " + System.getProperty("user.name") + " is : " + maxOpen);
        }

        AppLogService.info("Starting the Elastic Search server node with these settings:");
        Map<String, String> map = _server.settings().getAsMap();
        List<String> keys = new ArrayList<String>(map.keySet());
        Collections.sort(keys);
        for (String key : keys)
        {
            AppLogService.info(" " + key + " : " + getValue(map, key));
        }

        _server.start();

        checkServerStatus();

        AppLogService.info("Elastic Search server is started.");
    }

    /**
     * Stop the server
     */
    public void stop()
    {
        _server.close();
    }

    /**
     * Get client
     * @return The client
     */
    public Client getClient()
    {
        return _server.client();
    }

    /**
     * Gets max open files
     * @param testDir The directory
     * @return The max open files
     */
    protected int maxOpenFiles(File testDir)
    {
        boolean bDirCreated = false;
        if (!testDir.exists())
        {
            bDirCreated = true;
            testDir.mkdirs();
        }
        List<RandomAccessFile> files = new ArrayList<RandomAccessFile>();
        try
        {
            while (true)
            {
                files.add(new RandomAccessFile(new File(testDir, "tmp" + files.size()), "rw"));
            }
        }
        catch (FileNotFoundException ioe)
        {
            int i = 0;
            for (RandomAccessFile raf : files)
            {
                try
                {
                    raf.close();
                }
                catch (IOException e)
                {
                    // ignore
                }
                new File(testDir, "tmp" + i++).delete();
            }
            if (bDirCreated)
            {
                deleteRecursively(testDir);
            }
        }
        return files.size();
    }

    /**
     * Delete recursively
     * @param root The root directory
     * @return true if deleted otherwyse false
     */
    protected boolean deleteRecursively(File root)
    {
        return deleteRecursively(root, true);
    }

    /**
     * Delete the supplied {@link java.io.File} - for directories, recursively
     * delete any nested directories or files as well.
     *     
     * @param root the root <code>File</code> to delete
     * @param deleteRoot whether or not to delete the root itself or just the
     * content of the root.
     * @return <code>true</code> if the <code>File</code> was deleted, otherwise
     * <code>false</code>
     */
    protected boolean deleteRecursively(File root, boolean deleteRoot)
    {
        if (root != null && root.exists())
        {
            if (root.isDirectory())
            {
                File[] children = root.listFiles();
                if (children != null)
                {
                    for (File aChildren : children)
                    {
                        deleteRecursively(aChildren);
                    }
                }
            }

            return !deleteRoot || root.delete();
        }
        return false;
    }

    /**
     * Returns the cluster health status
     * @return the cluster health status
     */
    protected ClusterHealthStatus getHealthStatus()
    {
        return getClient().admin().cluster().prepareHealth().execute().actionGet().getStatus();
    }

    /**
     * Check the server status
     */
    protected void checkServerStatus()
    {
        ClusterHealthStatus status = getHealthStatus();

        // Check the current status of the ES cluster.
        if (ClusterHealthStatus.RED.equals(status))
        {
            AppLogService.info("ES cluster status is " + status + ". Waiting for ES recovery.");

            // Waits at most 30 seconds to make sure the cluster health is at least yellow.
            getClient().admin().cluster().prepareHealth()
                    .setWaitForYellowStatus()
                    .setTimeout("30s")
                    .execute().actionGet();
        }

        // Check the cluster health for a time.
        status = getHealthStatus();
        AppLogService.info("ES cluster status is " + status);

        // If we are still in red status, then we cannot proceed.
        if (ClusterHealthStatus.RED.equals(status))
        {
            throw new RuntimeException("ES cluster health status is RED. Server is not able to start.");
        }

    }

    /**
     * Get s property value
     * @param map The map
     * @param key The key
     * @return The value
     */
    protected static String getValue(Map<String, String> map, String key)
    {
        if (key.startsWith("cloud.aws.secret"))
        {
            return "<HIDDEN>";
        }
        return map.get(key);
    }
}
