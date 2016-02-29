package com.manyi.mqservice.util;

import java.io.File;
import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class MqClassLoader extends ClassLoader {

    public static ConcurrentHashMap<String, Class<?>> classes = new ConcurrentHashMap<String, Class<?>>();

    public static MqClassLoader instance = new MqClassLoader();

    private static String baseDir = MqClassLoader.class.getResource("/").getPath();

    private static String reloadableKw = "mqservice.reload";

    private static ReentrantLock lock = new ReentrantLock();

    public MqClassLoader() {
	super(Thread.currentThread().getContextClassLoader());
    }

    /**
     * 将类文件载入到命名空间
     * 
     * @param name
     *            类的全名 :
     * @param data
     *            class文件的byte[]
     */
    private Class<?> load(String name, byte[] data, boolean resolve) {
	Class<?> klass = defineClass(name, data, 0, data.length);
	if (resolve)
	    resolveClass(klass);
	classes.put(name, klass);
	return klass;
    }

    /**
     * 强制载入一个类
     * 
     * @param name
     *            类名
     */
    public static void reloadClass(String name, boolean resolve) throws ClassNotFoundException {
	String[] names = { name };
	reloadClasses(names, resolve);
    }

    /**
     * 强制载入一批类
     * 
     * @param wildnames
     *            可以是包名，也可以是类名的数组
     */
    private static void reloadClasses(String[] wildnames, boolean resolve) throws ClassNotFoundException {

	HashSet<File> file_pool = new HashSet<File>();
	HashSet<String> name_pool = new HashSet<String>();

	for (int i = 0; i < wildnames.length; i++) {
	    if (wildnames[i].endsWith(".*")) { // package
		String package_name = wildnames[i].substring(0, wildnames[i].length() - ".*".length());
		File[] files = find_package(package_name);
		String[] names = to_jvm_name(package_name);
		for (int j = 0; j < files.length; j++)
		    file_pool.add(files[j]);
		for (int j = 0; j < names.length; j++)
		    name_pool.add(names[j]);
	    } else { // individual class
		file_pool.add(findClassFile(wildnames[i]));
		name_pool.add(wildnames[i]);
	    }
	}

	reload(file_pool, name_pool, resolve);
    }

    /**
     * Force loading a package. Consider using
     * {@link #reloadClasses(String[], boolean)} for efficiency.
     * 
     * @see #reloadClasses(String[], boolean)
     */
    public static void reloadPackage(String name, boolean resolve) throws ClassNotFoundException {

	String[] names = { name + ".*" };
	reloadClasses(names, resolve);
    }

    public static Object INVALID = new Object();

    /**
     * 强制装载一组文件 每次reload会产生一个新的classloader的实例，该实例不会被销毁，除非所有这个实例装载的类全部被回收。
     * 因此重载类的请求应该作为一组内容放在一起，替代分开加载
     * 
     * @param names
     *            fully-qualified class names
     */
    private static void reload(HashSet<File> files, HashSet<String> names, boolean resolve)
	    throws ClassNotFoundException {

	MqClassLoader dummy = new MqClassLoader();

	for (Iterator file_it = files.iterator(), name_it = names.iterator(); file_it.hasNext();) {

	    byte[] data = read((File) file_it.next());
	    if (data == null)
		throw new ClassNotFoundException();

	    /*
	     * 在类里面可能存在类之间的依赖，比如，装载A
	     * 同时也需要装载B，但是装载B的时候将会产生linkageError，因为一个classloader只能装载
	     * 同一个类一次，我们可以忽略类似错误。 另一种方法是，保持对类的跟踪，看那些已经装载了，哪些正在装载，但是这些都是没有必要的。
	     */
	    try {
		dummy.load((String) name_it.next(), data, resolve);
	    } catch (LinkageError e) {
		// ignored
	    }
	}
    }

    /**
     * Load class facility for JVM.
     */
    public Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
	Object value = classes.get(name); // 检查缓存
	if (value != null && value != INVALID) {
	    Class<?> klass = (Class<?>) value;
	    if (resolve)
		resolveClass(klass);
	    return klass;

	} else { // 缓存中不存在
	    byte[] data = read(findClassFile(name)); // 读取类文件
	    if (data == null)
		return super.loadClass(name, resolve); // 交由父classloader去load类文件
	    else {
		try {
		    lock.lock();
		    Object cc = classes.get(name); // 检查缓存
		    if (cc != null) {
			return (Class<?>) cc;
		    } else
			return instance.load(name, data, resolve); // 自己load类文件
		} finally {
		    lock.unlock();
		}
	    }
	}
    }

    /**
     * 根据类名找到class文件对象 如果类名称中不包括允许reload的关键字,则返回空，委托给上一层的classloader去查找
     */
    private static File findClassFile(String name) {
	if (name.indexOf(reloadableKw) == -1) {
	    return null;
	}
	StringBuilder sb = new StringBuilder(baseDir);
	name = name.replace('.', File.separatorChar) + ".class";
	sb.append(File.separator + name);
	return new File(sb.toString());
    }

    /**
     * 返回package下面的所有文件
     */
    private static File[] find_package(String name) {
	StringBuilder sb = new StringBuilder(baseDir);
	name = name.replace('.', File.separatorChar);
	sb.append(File.separator + name);
	File dir = new File(sb.toString());
	if (!dir.exists()) {
	    return null;
	}
	String[] files = dir.list();
	File[] fs = new File[files.length];
	for (int i = 0; i < fs.length; i++) {
	    fs[i] = new File(files[i]);

	}
	return fs;
    }

    /**
     * 根据路径名称返回java格式类名
     */
    private static String[] to_jvm_name(String name) {
	StringBuilder sb = new StringBuilder(baseDir);
	name = name.replace('.', File.separatorChar);
	sb.append(File.separator + name);
	File dir = new File(sb.toString());
	if (!dir.exists()) {
	    return null;
	}
	String[] files = dir.list();
	for (String string : files) {
	    string = string.substring(string.indexOf(baseDir) + 1);
	    string = string.replace(File.separatorChar, '.');
	    string = string.substring(0, string.indexOf(".class"));
	}
	return files;
    }

    /**
     * Read a class file.
     * 
     * @param file
     *            类文件对象
     * 
     * @return byte数组
     */
    private static byte[] read(File file) {
	if (file == null || !file.exists())
	    return null;
	byte[] data = new byte[(int) file.length()];

	try {
	    new java.io.FileInputStream(file).read(data);
	    return data;

	} catch (java.io.IOException e) {
	    return null;
	}
    }

    /**
     * 重新初始化classloader
     */
    public static void reset() {
	instance = new MqClassLoader();
	classes.clear();
    }

    public static String getReloadableKw() {
	return reloadableKw;
    }

    public static void setReloadableKw(String reloadableKw) {
	MqClassLoader.reloadableKw = reloadableKw;
    }
}
