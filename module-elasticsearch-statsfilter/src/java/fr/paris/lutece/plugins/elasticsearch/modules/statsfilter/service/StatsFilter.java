/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package fr.paris.lutece.plugins.elasticsearch.modules.statsfilter.service;

import java.io.IOException;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

/**
 *
 */
public class StatsFilter implements Filter
{

    @Override
    public void init(FilterConfig fc) throws ServletException
    {
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException
    {
        HttpServletRequest r = (HttpServletRequest) request;
        System.out.println( r.getRequestURI() + r.getQueryString() );
        chain.doFilter( request, response );
    }

    @Override
    public void destroy()
    {
    }
    
}
