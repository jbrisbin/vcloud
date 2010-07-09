/*
 * Copyright (c) 2010 by J. Brisbin <jon@jbrisbin.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.jbrisbin.vcloud.cache;

/**
 * @author J. Brisbin <jon@jbrisbin.com>
 */
public interface AsyncCache {

  /**
   * Set the cloud-wide ID that is unique to this cache.
   *
   * @param id
   */
  public void setId( String id );

  /**
   * Get the cloud-wide ID that is unique to this cache.
   *
   * @return cloud-unique ID
   */
  public String getId();

  /**
   * Add an object to this particular node of the distributed cache.
   *
   * @param id  object ID to keep track of
   * @param obj the object to add
   */
  public void add( String id, Object obj );

  /**
   * Add an object to this node of the cache with a given expiration timeout value.
   *
   * @param id     object ID to keep track of
   * @param obj    the object to add
   * @param expiry how long should object exist in cache
   */
  public void add( String id, Object obj, long expiry );

  /**
   * Add a hierarchical relationship between two objects.
   *
   * @param childId  the ID of the child
   * @param parentId the ID of the parent
   */
  public void setParent( String childId, String parentId );

  /**
   * Remove an object from this particular node.
   *
   * @param id object ID to remove
   * @return true if object was removed, false otherwise
   */
  public void remove( String id );

  /**
   * Remove an object after the given timeout value.
   *
   * @param id    object ID to remove
   * @param delay how long to wait until removing object from cache
   * @return true if object will be removed, false otherwise
   */
  public void remove( String id, long delay );

  public void load( String id, AsyncCacheCallback callback );

  /**
   * Clear cache.
   */
  public void clear();

  /**
   * Start any resources required for this cache.
   */
  public void start();

  /**
   * Stop resources and clean up.
   */
  public void stop();

  /**
   * Stop resources and interrupt threads if running.
   *
   * @param interruptIfRunning
   */
  public void stop( boolean interruptIfRunning );

  /**
   * Whether this cache is still doing work or not.
   *
   * @return
   */
  public boolean isActive();

  public void setActive( boolean active );
}
