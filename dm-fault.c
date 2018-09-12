/*
 * dm-fault.c
 * Copyright (c) 2018 Jan Löser, SUSE Linux GmbH
 *
 * This file is released under the GPL.
 */

#include <linux/device-mapper.h>

#include <linux/module.h>
#include <linux/init.h>
#include <linux/blkdev.h>
#include <linux/bio.h>
#include <linux/slab.h>
#include <linux/debugfs.h>

#define DM_MSG_PREFIX "fault"

struct dentry *fault_debugfs;

struct per_bio_data {
	bool bio_submitted;
};

struct fault_lba {
	struct list_head next_fault;
	struct dentry *debugfs;
	bool active;
	sector_t fault_lba;
	void *fault_data;
	size_t fault_size;
};

struct fault_c {
	struct dm_dev *dev;
	struct dentry *debugfs;
	unsigned int num_lbas;
	unsigned int sector_size;
	sector_t start;
	struct list_head faults;
};

struct bio_p {
	struct fault_lba *lba;
	struct page *page;
};

static ssize_t read_fault_data(struct file *filp, char __user *buffer,
		size_t count, loff_t *ppos)
{
	struct fault_lba *fl = filp->private_data;

	if (!fl->active)
		return -EAGAIN;
	return simple_read_from_buffer(buffer, count, ppos,
			fl->fault_data, fl->fault_size);
}

static ssize_t write_fault_data(struct file *filp, const char __user *buffer,
		size_t count, loff_t *ppos)
{
	struct fault_lba *fl = filp->private_data;

	fl->active = true;
	return simple_write_to_buffer(fl->fault_data, fl->fault_size, ppos,
			buffer, count);
}

static const struct file_operations fops_fault_data = {
	.read = read_fault_data,
	.write = write_fault_data,
	.open = simple_open,
	.llseek = default_llseek,
};

static void read_lba_end_io(struct bio *bio)
{
	struct fault_lba *fl = bio->bi_private;
	struct page *page = bio->bi_io_vec[0].bv_page;
	char *data = page_address(page);

	memcpy(fl->fault_data, data, fl->fault_size);
	printk("bio: inital read sector=%d\n", fl->fault_lba);
	fl->active = true;
}

static void read_lba(struct fault_lba *fl, struct fault_c *fc)
{
	struct bio *bio;
	struct page *page;

	bio = bio_alloc(GFP_NOIO, 1);
	if (!bio) {
		DMERR("couldn't allocate bio");
		return NULL;
	}
	page = alloc_page(GFP_NOIO);
	if (!page) {
		DMERR("couldn't allocate page");
		return NULL;
	}

	bio->bi_iter.bi_sector = fl->fault_lba;
	bio_set_dev(bio, fc->dev->bdev);
	bio->bi_private = fl;
	bio->bi_end_io = read_lba_end_io;
	bio_set_op_attrs(bio, REQ_OP_READ, REQ_SYNC);
	bio_add_page(bio, page, fc->sector_size, 0);
	submit_bio(bio);
}

/*
 * Construct a fault mapping:
 * <dev_path> <offset> <sector_size> <num lbas> [<lba>]*
 *
 */
static int fault_ctr(struct dm_target *ti, unsigned int argc, char **argv)
{
	static struct dm_arg _args[] = {
		{1, UINT_MAX, "Invalid # of lbas"},
	};

	int r, i = 0;
	struct fault_c *fc;
	struct fault_lba *fl, *tmp;
	unsigned long long tmpll;
	struct dm_arg_set as;
	const char *devname, *arg;
	char dummy;

	as.argc = argc;
	as.argv = argv;

	if (argc < 4) {
		ti->error = "Invalid argument count";
		return -EINVAL;
	}

	fc = kzalloc(sizeof(*fc), GFP_KERNEL);
	if (!fc) {
		ti->error = "Cannot allocate context";
		return -ENOMEM;
   }

	devname = dm_shift_arg(&as);

	r = dm_get_device(ti, devname, dm_table_get_mode(ti->table), &fc->dev);
	if (r) {
		ti->error = "Device lookup failed";
		kfree(fc);
		return r;
	}

	r = -EINVAL;
	arg = dm_shift_arg(&as);
	if (!arg || sscanf(arg, "%llu%c", &tmpll, &dummy) != 1) {
		ti->error = "Invalid device sector";
		goto bad;
	}
	fc->start = tmpll;
	INIT_LIST_HEAD(&fc->faults);

	fc->debugfs = debugfs_create_dir(fc->dev->name, fault_debugfs);
	if (IS_ERR(fc->debugfs)) {
		ti->error = "Cannot create debugfs dir";
		r = PTR_ERR(fc->debugfs);
		goto bad;
	}

	r = dm_read_arg(_args, &as, &fc->sector_size, &ti->error);
	if (r)
		goto bad;

	r = dm_read_arg(_args, &as, &fc->num_lbas, &ti->error);
	if (r)
	goto bad;

	while (i < fc->num_lbas) {
		char fault_dname[256];

		r = -ENOMEM;
		fl = kzalloc(sizeof(*fl), GFP_KERNEL);
		if (!fl) {
			ti->error = "Cannot allocate fault lba";
			goto bad;
		}
		fl->active = false;
		fl->fault_data = kzalloc(fc->sector_size, GFP_KERNEL);
		if (!fl->fault_data) {
			ti->error = "Cannot allocate fault lba data";
			kfree(fl);
			goto bad;
		}

		fl->fault_size = fc->sector_size;
		arg = dm_shift_arg(&as);
		if (!arg || sscanf(arg, "%llu%c", &tmpll, &dummy) != 1) {
			ti->error = "Invalid fault lba sector";
			kfree(fl->fault_data);
			kfree(fl);
			r = -EINVAL;
			goto bad;
		}
		fl->fault_lba = tmpll;
		sprintf(fault_dname, "%llu", (unsigned long long)fl->fault_lba);
		fl->debugfs = debugfs_create_file(fault_dname,
				S_IRUSR | S_IWUSR, fc->debugfs,
				fl, &fops_fault_data);
		if (IS_ERR(fl->debugfs)) {
			ti->error = "Cannot create debugfs dentry";
			r = PTR_ERR(fl->debugfs);
			kfree(fl->fault_data);
			kfree(fl);
			goto bad;
		}
		read_lba(fl, fc);
		list_add_tail(&fl->next_fault, &fc->faults);
		i++;
	}

	ti->num_flush_bios = 1;
	ti->num_discard_bios = 1;
	ti->per_io_data_size = sizeof(struct per_bio_data);
	ti->private = fc;
	return 0;

bad:
	list_for_each_entry_safe(fl, tmp, &fc->faults, next_fault) {
		list_del_init(&fl->next_fault);
		debugfs_remove(fl->debugfs);
		kfree(fl->fault_data);
		kfree(fl);
	}
	debugfs_remove_recursive(fc->debugfs);
	dm_put_device(ti, fc->dev);
	kfree(fc);
	return r;
}

static void fault_dtr(struct dm_target *ti)
{
	struct fault_c *fc = ti->private;
	struct fault_lba *fl, *tmp;

	dm_put_device(ti, fc->dev);
	list_for_each_entry_safe(fl, tmp, &fc->faults, next_fault) {
		list_del_init(&fl->next_fault);
		debugfs_remove(fl->debugfs);
		kfree(fl->fault_data);
		kfree(fl);
	}
	debugfs_remove(fc->debugfs);
	kfree(fc);
}

static sector_t fault_map_sector(struct dm_target *ti, sector_t bi_sector)
{
	struct fault_c *fc = ti->private;

	return fc->start + dm_target_offset(ti, bi_sector);
}

static void fault_map_bio(struct dm_target *ti, struct bio *bio)
{
	struct fault_c *fc = ti->private;

	bio_set_dev(bio, fc->dev->bdev);
	if (bio_sectors(bio))
		bio->bi_iter.bi_sector =
			fault_map_sector(ti, bio->bi_iter.bi_sector);
}

static int fault_map(struct dm_target *ti, struct bio *bio)
{
	struct fault_c *fc = ti->private;
	struct per_bio_data *pb = dm_per_bio_data(bio, sizeof(struct per_bio_data));
	pb->bio_submitted = false;

	if (bio_data_dir(bio) == READ) {
		pb->bio_submitted = true;
	}

map_bio:
	fault_map_bio(ti, bio);

	return DM_MAPIO_REMAPPED;
}

static void corrupt_bio_data(struct bio *bio, struct fault_c *fc)
{
	struct fault_lba *fl;
	unsigned bio_bytes;
	sector_t bio_sector;
	unsigned sectors;
	unsigned bio_offset;
	char *data;

	bio_rewind_iter(bio, &bio->bi_iter, bio->bi_iter.bi_done);

	data = bio_data(bio);
	bio_bytes = bio_cur_bytes(bio);
	bio_sector = bio->bi_iter.bi_sector;
	bio_offset = bio_offset(bio);
	sectors = bio_bytes >> 9;

	list_for_each_entry(fl, &fc->faults, next_fault) {
		if (!fl->active)
			continue;

		if ((fl->fault_lba >= bio_sector) &&
				(fl->fault_lba < (bio_sector + sectors))) {
			unsigned offset = (fl->fault_lba - bio_sector) * fl->fault_size;
			printk("bio: modified sector=%d start_sector=%d sectors=%d offset=%d\n",
				fl->fault_lba, bio_sector, sectors, offset);
			memcpy(data + offset, fl->fault_data , fl->fault_size);
		}
	}
}

static int fault_end_io(struct dm_target *ti, struct bio *bio, blk_status_t *error)
{
	struct fault_c *fc = ti->private;
	struct per_bio_data *pb = dm_per_bio_data(bio, sizeof(struct per_bio_data));

	if (!*error && pb->bio_submitted && (bio_data_dir(bio) == READ))
		corrupt_bio_data(bio, fc);

	return DM_ENDIO_DONE;
}

static void fault_status(struct dm_target *ti, status_type_t type,
		unsigned status_flags, char *result, unsigned maxlen)
{
	unsigned sz = 0;
	struct fault_c *fc = ti->private;
	struct fault_lba *fl;

	switch (type) {
	case STATUSTYPE_INFO:
		DMEMIT("%s %llu %llu %u ", fc->dev->name,
				(unsigned long long)fc->start,
				(unsigned long long)fc->sector_size, fc->num_lbas);

		list_for_each_entry(fl, &fc->faults, next_fault)
			DMEMIT("%d ", fl->active);

		break;

	case STATUSTYPE_TABLE:
		DMEMIT("%s %llu %llu %u ", fc->dev->name,
				(unsigned long long)fc->start,
				(unsigned long long)fc->sector_size, fc->num_lbas);

		list_for_each_entry(fl, &fc->faults, next_fault)
			DMEMIT("%llu ", (unsigned long long)fl->fault_lba);

		break;
	}
}

static int fault_prepare_ioctl(struct dm_target *ti,
		struct block_device **bdev, fmode_t *mode)
{
	struct fault_c *fc = ti->private;

	*bdev = fc->dev->bdev;

	/*
	* Only pass ioctls through if the device sizes match exactly.
	*/
	if (fc->start ||
			ti->len != i_size_read((*bdev)->bd_inode) >> SECTOR_SHIFT)
		return 1;
	return 0;
}

static int fault_iterate_devices(struct dm_target *ti,
		iterate_devices_callout_fn fn, void *data)
{
	struct fault_c *fc = ti->private;

	return fn(ti, fc->dev, fc->start, ti->len, data);
}

static struct target_type fault_target = {
	.name   = "fault",
	.version = {1, 0, 0},
	.module = THIS_MODULE,
	.ctr    = fault_ctr,
	.dtr    = fault_dtr,
	.map    = fault_map,
	.end_io = fault_end_io,
	.status = fault_status,
	.prepare_ioctl = fault_prepare_ioctl,
	.iterate_devices = fault_iterate_devices,
};

static int __init dm_fault_init(void)
{
	int r;

	fault_debugfs = debugfs_create_dir("dm-fault", NULL);
	if (!fault_debugfs) {
		DMERR("debugfs create failed");
		return -ENOMEM;
	}

	r = dm_register_target(&fault_target);
	if (r < 0)
		DMERR("register failed %d", r);

	return r;
}

static void __exit dm_fault_exit(void)
{
	debugfs_remove_recursive(fault_debugfs);
	dm_unregister_target(&fault_target);
}

/* Module hooks */
module_init(dm_fault_init);
module_exit(dm_fault_exit);

MODULE_DESCRIPTION(DM_NAME " fault target");
MODULE_AUTHOR("Jan Löser <jloeser@suse.com>");
MODULE_LICENSE("GPL");
