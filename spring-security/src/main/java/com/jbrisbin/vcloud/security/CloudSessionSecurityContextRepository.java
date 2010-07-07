package com.jbrisbin.vcloud.security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.authentication.AuthenticationTrustResolver;
import org.springframework.security.authentication.AuthenticationTrustResolverImpl;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.web.context.HttpRequestResponseHolder;
import org.springframework.security.web.context.SaveContextOnUpdateOrErrorResponseWrapper;
import org.springframework.security.web.context.SecurityContextRepository;
import org.springframework.util.ReflectionUtils;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

/**
 * Much of this had to be absconded from HttpSessionSecurityContextRepository because everything in there is "private"
 * when it should be "protected" and overridable. Argh!
 *
 * @author Jon Brisbin <jon.brisbin@npcinternational.com>
 */
public class CloudSessionSecurityContextRepository implements SecurityContextRepository {

  protected final Logger log = LoggerFactory.getLogger(getClass());
  protected final boolean debug = log.isDebugEnabled();
  protected String sessionAttributeName = "CLOUD_SESSION_SECURITY_CONTEXT";
  protected String securityContextClass = null;
  protected boolean disableUrlRewriting = false;
  protected AuthenticationTrustResolver authenticationTrustResolver = new AuthenticationTrustResolverImpl();

  public String getSessionAttributeName() {
    return sessionAttributeName;
  }

  public void setSessionAttributeName(String sessionAttributeName) {
    this.sessionAttributeName = sessionAttributeName;
  }

  public String getSecurityContextClass() {
    return securityContextClass;
  }

  public void setSecurityContextClass(String securityContextClass) {
    this.securityContextClass = securityContextClass;
  }

  public boolean isDisableUrlRewriting() {
    return disableUrlRewriting;
  }

  public void setDisableUrlRewriting(boolean disableUrlRewriting) {
    this.disableUrlRewriting = disableUrlRewriting;
  }

  @Override
  public SecurityContext loadContext(HttpRequestResponseHolder httpRequestResponseHolder) {
    HttpServletRequest request = httpRequestResponseHolder.getRequest();
    HttpServletResponse response = httpRequestResponseHolder.getResponse();
    HttpSession session = request.getSession();

    SecurityContext context;
    Object obj = session.getAttribute(sessionAttributeName);
    if (null != obj && obj instanceof SecurityContext) {
      if (debug) {
        log.debug("Found existing SecurityContext in session " + session.getId());
      }
      context = (SecurityContext) obj;
    } else {
      if (debug) {
        log.debug("Returning new SecurityContext instance.");
      }
      context = createSecurityContext();
    }

    httpRequestResponseHolder.setResponse(new SaveToSessionResponseWrapper(response,
        request,
        (null != session),
        context));

    return context;
  }

  @Override
  public void saveContext(SecurityContext securityContext, HttpServletRequest request, HttpServletResponse response) {
    if (debug) {
      log.debug("Asked to save SecurityContext " + securityContext.toString());
    }
    request.getSession().setAttribute(sessionAttributeName, securityContext);
  }

  @Override
  public boolean containsContext(HttpServletRequest request) {
    return (null != request.getSession().getAttribute(sessionAttributeName));
  }

  protected SecurityContext createSecurityContext() {
    SecurityContext context = null;
    if (null != securityContextClass) {
      try {
        context = (SecurityContext) Class.forName(securityContextClass).newInstance();
      } catch (Exception e) {
        ReflectionUtils.handleReflectionException(e);
      }
    }
    if (null == context) {
      context = SecurityContextHolder.createEmptyContext();
    }

    return context;
  }

  /**
   * Pilfered from HttpSessionSecurityContextRepository because that inner class is private. Argh!
   */
  class SaveToSessionResponseWrapper extends SaveContextOnUpdateOrErrorResponseWrapper {

    private HttpServletRequest request;
    private boolean httpSessionExistedAtStartOfRequest;
    private SecurityContext contextBeforeExecution;
    private Authentication authBeforeExecution;

    SaveToSessionResponseWrapper(HttpServletResponse response, HttpServletRequest request,
                                 boolean httpSessionExistedAtStartOfRequest,
                                 SecurityContext context) {
      super(response, disableUrlRewriting);
      this.request = request;
      this.httpSessionExistedAtStartOfRequest = httpSessionExistedAtStartOfRequest;
      this.contextBeforeExecution = context;
      this.authBeforeExecution = context.getAuthentication();
    }

    @Override
    protected void saveContext(SecurityContext securityContext) {
      // See SEC-776
      if (authenticationTrustResolver.isAnonymous(securityContext.getAuthentication())) {
        if (debug) {
          log.debug("SecurityContext contents are anonymous - context will not be stored in HttpSession.");
        }
        return;
      }

      HttpSession httpSession = request.getSession();
      SecurityContext contextFromSession = (SecurityContext) httpSession.getAttribute(sessionAttributeName);
      if (securityContext != contextFromSession) {
        if (securityContext != contextBeforeExecution || securityContext.getAuthentication() != authBeforeExecution) {
          httpSession.setAttribute(sessionAttributeName, securityContext);
          if (debug) {
            log.debug("SecurityContext stored to HttpSession: '" + securityContext + "'");
          }
        }
      }
    }
  }

}
