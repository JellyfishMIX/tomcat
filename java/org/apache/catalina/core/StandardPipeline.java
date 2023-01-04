/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.catalina.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.management.ObjectName;

import org.apache.catalina.Contained;
import org.apache.catalina.Container;
import org.apache.catalina.JmxEnabled;
import org.apache.catalina.Lifecycle;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.LifecycleState;
import org.apache.catalina.Pipeline;
import org.apache.catalina.Valve;
import org.apache.catalina.util.LifecycleBase;
import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;
import org.apache.tomcat.util.ExceptionUtils;
import org.apache.tomcat.util.res.StringManager;

/**
 * Standard implementation of a processing <b>Pipeline</b> that will invoke
 * a series of Valves that have been configured to be called in order.  This
 * implementation can be used for any type of Container.
 *
 * <b>IMPLEMENTATION WARNING</b> - This implementation assumes that no
 * calls to <code>addValve()</code> or <code>removeValve</code> are allowed
 * while a request is currently being processed.  Otherwise, the mechanism
 * by which per-thread state is maintained will need to be modified.
 *
 * @author Craig R. McClanahan
 */
public class StandardPipeline extends LifecycleBase implements Pipeline {

    private static final Log log = LogFactory.getLog(StandardPipeline.class);
    private static final StringManager sm = StringManager.getManager(StandardPipeline.class);

    // ----------------------------------------------------------- Constructors


    /**
     * Construct a new StandardPipeline instance with no associated Container.
     */
    public StandardPipeline() {

        this(null);

    }


    /**
     * Construct a new StandardPipeline instance that is associated with the
     * specified Container.
     *
     * @param container The container we should be associated with
     */
    public StandardPipeline(Container container) {

        super();
        setContainer(container);

    }


    // ----------------------------------------------------- Instance Variables


    /**
     * The basic Valve (if any) associated with this Pipeline.
     */
    protected Valve basic = null;


    /**
     * The Container with which this Pipeline is associated.
     */
    protected Container container = null;


    /**
     * The first valve associated with this Pipeline.
     */
    protected Valve first = null;


    // --------------------------------------------------------- Public Methods

    @Override
    public boolean isAsyncSupported() {
        Valve valve = (first!=null)?first:basic;
        boolean supported = true;
        while (supported && valve!=null) {
            supported = supported & valve.isAsyncSupported();
            valve = valve.getNext();
        }
        return supported;
    }


    @Override
    public void findNonAsyncValves(Set<String> result) {
        Valve valve = (first!=null) ? first : basic;
        while (valve != null) {
            if (!valve.isAsyncSupported()) {
                result.add(valve.getClass().getName());
            }
            valve = valve.getNext();
        }
    }


    // ------------------------------------------------------ Contained Methods

    /**
     * Return the Container with which this Pipeline is associated.
     */
    @Override
    public Container getContainer() {
        return this.container;
    }


    /**
     * Set the Container with which this Pipeline is associated.
     *
     * @param container The new associated container
     */
    @Override
    public void setContainer(Container container) {
        this.container = container;
    }


    @Override
    protected void initInternal() {
        // NOOP
    }


    /**
     * Start {@link Valve}s) in this pipeline and implement the requirements
     * of {@link LifecycleBase#startInternal()}.
     *
     * @exception LifecycleException if this component detects a fatal error
     *  that prevents this component from being used
     */
    @Override
    protected synchronized void startInternal() throws LifecycleException {

        // Start the Valves in our pipeline (including the basic), if any
        // 设置当前 valve，如果 first 引用指向了 valve，则 first 指向的 valve 为当前 valve。否则 basicValve 为当前 valve。
        Valve current = first;
        if (current == null) {
            current = basic;
        }
        // 遍历此 pipeline 的 valve 链表，调用链上每个 valve 的 start 方法
        while (current != null) {
            if (current instanceof Lifecycle) {
                ((Lifecycle) current).start();
            }
            current = current.getNext();
        }
        // 将 pipeline 状态置为 STARTING
        setState(LifecycleState.STARTING);
    }


    /**
     * Stop {@link Valve}s) in this pipeline and implement the requirements
     * of {@link LifecycleBase#stopInternal()}.
     *
     * @exception LifecycleException if this component detects a fatal error
     *  that prevents this component from being used
     */
    @Override
    protected synchronized void stopInternal() throws LifecycleException {

        setState(LifecycleState.STOPPING);

        // Stop the Valves in our pipeline (including the basic), if any
        Valve current = first;
        if (current == null) {
            current = basic;
        }
        while (current != null) {
            if (current instanceof Lifecycle) {
                ((Lifecycle) current).stop();
            }
            current = current.getNext();
        }
    }


    @Override
    protected void destroyInternal() {
        Valve[] valves = getValves();
        for (Valve valve : valves) {
            removeValve(valve);
        }
    }


    /**
     * Return a String representation of this component.
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Pipeline[");
        sb.append(container);
        sb.append(']');
        return sb.toString();
    }


    // ------------------------------------------------------- Pipeline Methods


    /**
     * <p>Return the Valve instance that has been distinguished as the basic
     * Valve for this Pipeline (if any).
     */
    @Override
    public Valve getBasic() {
        return this.basic;
    }


    /**
     * <p>Set the Valve instance that has been distinguished as the basic
     * Valve for this Pipeline (if any).  Prior to setting the basic Valve,
     * the Valve's <code>setContainer()</code> will be called, if it
     * implements <code>Contained</code>, with the owning Container as an
     * argument.  The method may throw an <code>IllegalArgumentException</code>
     * if this Valve chooses not to be associated with this Container, or
     * <code>IllegalStateException</code> if it is already associated with
     * a different Container.</p>
     *
     * 设置基础阀门 basicValve
     *
     * @param valve Valve to be distinguished as the basic Valve
     */
    @Override
    public void setBasic(Valve valve) {

        // Change components if necessary
        // 如果已经有 basicValve 并且和要设置的值一样，那么直接 return 结束设置
        Valve oldBasic = this.basic;
        if (oldBasic == valve) {
            return;
        }

        // Stop the old component if necessary
        // 如果旧的 basicValve 非空，则调用其 stop 方法，取消和对应 container 的关联
        if (oldBasic != null) {
            if (getState().isAvailable() && (oldBasic instanceof Lifecycle)) {
                try {
                    ((Lifecycle) oldBasic).stop();
                } catch (LifecycleException e) {
                    log.error(sm.getString("standardPipeline.basic.stop"), e);
                }
            }
            if (oldBasic instanceof Contained) {
                try {
                    ((Contained) oldBasic).setContainer(null);
                } catch (Throwable t) {
                    ExceptionUtils.handleThrowable(t);
                }
            }
        }

        // Start the new component if necessary
        if (valve == null) {
            return;
        }
        // 新的 basicValve 和 container 进行关联
        if (valve instanceof Contained) {
            ((Contained) valve).setContainer(this.container);
        }
        // 启动新的 basicValve
        if (getState().isAvailable() && valve instanceof Lifecycle) {
            try {
                ((Lifecycle) valve).start();
            } catch (LifecycleException e) {
                log.error(sm.getString("standardPipeline.basic.start"), e);
                return;
            }
        }

        // Update the pipeline
        // 遍历阀门链表，将新的 basicValve 替换至阀门链表
        Valve current = first;
        while (current != null) {
            if (current.getNext() == oldBasic) {
                current.setNext(valve);
                break;
            }
            current = current.getNext();
        }

        this.basic = valve;

    }


    /**
     * <p>Add a new Valve to the end of the pipeline associated with this
     * Container.  Prior to adding the Valve, the Valve's
     * <code>setContainer()</code> method will be called, if it implements
     * <code>Contained</code>, with the owning Container as an argument.
     * The method may throw an
     * <code>IllegalArgumentException</code> if this Valve chooses not to
     * be associated with this Container, or <code>IllegalStateException</code>
     * if it is already associated with a different Container.</p>
     *
     * 添加 valve
     *
     * @param valve Valve to be added
     *
     * @exception IllegalArgumentException if this Container refused to
     *  accept the specified Valve
     * @exception IllegalArgumentException if the specified Valve refuses to be
     *  associated with this Container
     * @exception IllegalStateException if the specified Valve is already
     *  associated with a different Container
     */
    @Override
    public void addValve(Valve valve) {

        // Validate that we can add this Valve
        // valve 和 container 进行关联
        if (valve instanceof Contained) {
            ((Contained) valve).setContainer(this.container);
        }

        // Start the new component if necessary
        // 启动 valve
        if (getState().isAvailable()) {
            if (valve instanceof Lifecycle) {
                try {
                    ((Lifecycle) valve).start();
                } catch (LifecycleException e) {
                    log.error(sm.getString("standardPipeline.valve.start"), e);
                }
            }
        }

        // Add this Valve to the set associated with this Pipeline
        // first 指向阀门链表第一个元素，从 first 开始遍历阀门链表，把 valve 加在现有 valve 链表末尾，basicValve 之前
        if (first == null) {
            first = valve;
            valve.setNext(basic);
        } else {
            Valve current = first;
            while (current != null) {
                if (current.getNext() == basic) {
                    current.setNext(valve);
                    valve.setNext(basic);
                    break;
                }
                current = current.getNext();
            }
        }
        // 触发 container 添加 valve 事件
        container.fireContainerEvent(Container.ADD_VALVE_EVENT, valve);
    }


    /**
     * Return the set of Valves in the pipeline associated with this
     * Container, including the basic Valve (if any).  If there are no
     * such Valves, a zero-length array is returned.
     *
     * 获取所有的 valve，数组形式返回
     */
    @Override
    public Valve[] getValves() {
        // 遍历链表把 valve 添加进集合内，最后集合转数组
        List<Valve> valveList = new ArrayList<>();
        Valve current = first;
        if (current == null) {
            current = basic;
        }
        while (current != null) {
            valveList.add(current);
            current = current.getNext();
        }

        return valveList.toArray(new Valve[0]);

    }

    public ObjectName[] getValveObjectNames() {

        List<ObjectName> valveList = new ArrayList<>();
        Valve current = first;
        if (current == null) {
            current = basic;
        }
        while (current != null) {
            if (current instanceof JmxEnabled) {
                valveList.add(((JmxEnabled) current).getObjectName());
            }
            current = current.getNext();
        }

        return valveList.toArray(new ObjectName[0]);

    }

    /**
     * Remove the specified Valve from the pipeline associated with this
     * Container, if it is found; otherwise, do nothing.  If the Valve is
     * found and removed, the Valve's <code>setContainer(null)</code> method
     * will be called if it implements <code>Contained</code>.
     *
     * 移除指定的 valve
     *
     * @param valve Valve to be removed
     */
    @Override
    public void removeValve(Valve valve) {

        Valve current;
        if(first == valve) {
            first = first.getNext();
            current = null;
        } else {
            current = first;
        }
        // 遍历阀门链表，定位目标 valve 并移除
        while (current != null) {
            if (current.getNext() == valve) {
                current.setNext(valve.getNext());
                break;
            }
            current = current.getNext();
        }
        // first 是严格定义的除了 basicValve 的第一个阀门，不包括 basicValve
        if (first == basic) {
            first = null;
        }

        // 取消被移除的 valve 与 container 的关联
        if (valve instanceof Contained) {
            ((Contained) valve).setContainer(null);
        }

        // 调用被移除 valve 的 stop, destroy 方法
        if (valve instanceof Lifecycle) {
            // Stop this valve if necessary
            if (getState().isAvailable()) {
                try {
                    ((Lifecycle) valve).stop();
                } catch (LifecycleException e) {
                    log.error(sm.getString("standardPipeline.valve.stop"), e);
                }
            }
            try {
                ((Lifecycle) valve).destroy();
            } catch (LifecycleException e) {
                log.error(sm.getString("standardPipeline.valve.destroy"), e);
            }
        }
        // 触发 container 移除 valve 事件。
        container.fireContainerEvent(Container.REMOVE_VALVE_EVENT, valve);
    }

    /**
     * 获取 pipeline 第一个 valve，如果没有普通 valve 则返回 basicValve
     */
    @Override
    public Valve getFirst() {
        if (first != null) {
            return first;
        }

        return basic;
    }
}
