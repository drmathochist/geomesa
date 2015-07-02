package org.locationtech.geomesa.web.csv

import org.geoserver.security.xml.XMLGeoserverUser
import org.springframework.security.authentication.AnonymousAuthenticationToken
import org.springframework.security.core.userdetails.UsernameNotFoundException
import org.springframework.security.core.Authentication
import org.springframework.security.core.context.SecurityContextHolder

trait UserAuthentication {
  def getUserAuth: Option[Authentication] =
    Option(SecurityContextHolder.getContext.getAuthentication) match {
      case None => None
      case Some(_: AnonymousAuthenticationToken) => None  // anonymous authentication should be the same as no authentication
      case authO@Some(_) => authO
    }

  def getUserName: Option[String]
}

trait BasicUserAuthentication extends UserAuthentication {
  def getUserName =
    for {userAuth <- getUserAuth} yield {
      userAuth.getPrincipal match {
        case str: String => str.toLowerCase
        case p => throw new UsernameNotFoundException(s"Expected String principal but found ${p.getClass.getCanonicalName}")
      }
    }
}

// can be moved to gs-ext
trait XMLGSUserAuthentication extends UserAuthentication {
  def getUserName =
    for {userAuth <- getUserAuth} yield {
      userAuth.getPrincipal match {
        case xml: XMLGeoserverUser => xml.getUsername
        case p => throw new UsernameNotFoundException(s"Expected XMLGeoserverUser principal but found ${p.getClass.getCanonicalName}")
      }
    }
}

// can be moved to gs-ext
trait FlexibleUserAuthentication extends UserAuthentication {
  def getUserName =
    for {userAuth <- getUserAuth} yield {
      userAuth.getPrincipal match {
        case str: String           => str.toLowerCase
        case xml: XMLGeoserverUser => xml.getUsername
        case p => throw new UsernameNotFoundException(s"Expected XMLGeoserverUser principal but found ${p.getClass.getCanonicalName}")
      }
    }
}